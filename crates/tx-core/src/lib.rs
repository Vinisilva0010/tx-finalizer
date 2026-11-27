use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Identificador idempotente único fornecido pelo chamador para evitar duplicação de intenção financeira.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdempotencyKey(pub String);

/// Metadados específicos de janela de validade e ordenação de cada blockchain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChainConstraint {
    Evm {
        chain_id: u64,
        nonce: u64,
    },
    Solana {
        recent_blockhash: String,
        last_valid_block_height: u64,
    },
}

/// Payload assinado imutável. Uma vez criado para uma tentativa, apenas estes exatos bytes podem ser reenviados.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedPayload {
    pub tx_hash: String,
    pub payload_sha256: [u8; 32],
    pub raw_bytes: Bytes,
    pub constraint: ChainConstraint,
}

impl SignedPayload {
    pub fn new(tx_hash: String, raw_bytes: Bytes, constraint: ChainConstraint) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(&raw_bytes);
        let digest = hasher.finalize();
        let mut payload_sha256 = [0u8; 32];
        payload_sha256.copy_from_slice(&digest);

        Self {
            tx_hash,
            payload_sha256,
            raw_bytes,
            constraint,
        }
    }

    pub fn verify_integrity(&self) -> bool {
        let mut hasher = Sha256::new();
        hasher.update(&self.raw_bytes);
        let digest = hasher.finalize();
        digest.as_slice() == self.payload_sha256
    }
}

/// Posição de inclusão provisória antes da finalidade econômica.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockInclusion {
    pub block_number: u64,
    pub block_hash: String,
}

/// Máquina de estados determinística da transação.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxState {
    /// Assinada e persistida, pronta para o primeiro disparo.
    Prepared,
    /// Disparada ou com timeout/erro de rede ambíguo; proibido gerar novos bytes.
    SubmittedUnknown { attempts: u32 },
    /// Incluída em bloco ainda sujeito a reorganização.
    IncludedTentative { inclusion: BlockInclusion },
    /// Removida por reorganização de blocos; retorna ao monitoramento/reenvio dos mesmos bytes.
    Reorged {
        previous_inclusion: BlockInclusion,
        reorg_count: u32,
    },
    /// Estado terminal: bloco atingiu finalidade na rede.
    Finalized { inclusion: BlockInclusion },
    /// Estado terminal: expirou na rede sem ser finalizada (seguro para emitir nova intenção se desejado).
    ExpiredOrFailed { reason: String },
}

impl TxState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, TxState::Finalized { .. } | TxState::ExpiredOrFailed { .. })
    }
}

/// Registro persistido que vincula a chave de idempotência ao payload assinado e seu estado atual.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxRecord {
    pub idempotency_key: IdempotencyKey,
    pub payload: SignedPayload,
    pub state: TxState,
    pub version: u64,
}

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("integridade dos bytes assinados violada para tx {0}")]
    PayloadIntegrityCorrupted(String),
    #[error("transição de estado inválida de {from:?} para {to:?}")]
    InvalidTransition { from: String, to: String },
    #[error("conflito de concorrência otimista ao salvar registro (versão esperada {expected})")]
    OptimisticLockConflict { expected: u64 },
    #[error("falha na camada de armazenamento: {0}")]
    Storage(String),
    #[error("falha na camada de assinatura: {0}")]
    Signer(String),
    #[error("erro definitivo da rede: {0}")]
    TerminalChainRejection(String),
}

/// Resultado de uma tentativa de broadcast no RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BroadcastOutcome {
    /// RPC aceitou o payload (ainda não significa inclusão nem finalidade).
    Accepted,
    /// Timeout, queda de conexão, HTTP 429/502/504: resultado desconhecido, exige reenvio idêntico.
    UnknownTimeout(String),
    /// Rejeição definitiva antes de entrar no mempool (ex: assinatura inválida, formato corrompido).
    DefinitiveReject(String),
}

/// Resultado da consulta de observação on-chain para um `tx_hash`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservationStatus {
    /// Ainda não encontrada nem expirada.
    Pending { current_block_height: u64 },
    /// Incluída em um bloco que ainda não atingiu finalidade.
    IncludedNotFinalized {
        inclusion: BlockInclusion,
        current_block_height: u64,
    },
    /// Incluída em um bloco já finalizado pela rede.
    Finalized { inclusion: BlockInclusion },
    /// A janela de validade da transação expirou sem inclusão (ex: blockhash expirado na Solana).
    Expired { current_block_height: u64 },
}

/// Contrato de persistência atômica com controle de concorrência otimista.
#[async_trait]
pub trait TxStore: Send + Sync {
    async fn get(&self, key: &IdempotencyKey) -> Result<Option<TxRecord>, CoreError>;
    /// Insere somente se a chave não existir; se já existir, retorna o registro existente sem sobrescrever.
    async fn insert_if_absent(&self, record: TxRecord) -> Result<TxRecord, CoreError>;
    /// Atualiza o registro somente se `record.version` corresponder à versão atual persistida.
    async fn compare_and_swap(&self, record: TxRecord) -> Result<TxRecord, CoreError>;
}

/// Contrato para assinar a intenção uma única vez antes de persistir.
#[async_trait]
pub trait TxSigner<Intent>: Send + Sync {
    async fn sign_once(&self, intent: &Intent) -> Result<SignedPayload, CoreError>;
}

/// Contrato de comunicação com a blockchain (EVM ou Solana).
#[async_trait]
pub trait ChainAdapter: Send + Sync {
    /// Envia estritamente `payload.raw_bytes`. Nunca altera nem re-assina.
    async fn broadcast_raw(&self, payload: &SignedPayload) -> BroadcastOutcome;
    /// Consulta o estado atual da transação e verifica finalidade e reorganizações.
    async fn observe(&self, payload: &SignedPayload) -> Result<ObservationStatus, CoreError>;
}

/// Orquestrador agnóstico de cadeia que aplica as invariantes de não-duplicação e finalidade.
pub struct TxEngine<S, C> {
    store: S,
    chain: C,
}

impl<S: TxStore, C: ChainAdapter> TxEngine<S, C> {
    pub fn new(store: S, chain: C) -> Self {
        Self { store, chain }
    }

    /// Garante que uma intenção de negócio gere no máximo um `SignedPayload` persistido.
    /// Chamadas concorrentes ou repetidas com a mesma `IdempotencyKey` retornam o mesmo registro.
    pub async fn prepare<Intent, Signer>(
        &self,
        key: IdempotencyKey,
        intent: &Intent,
        signer: &Signer,
    ) -> Result<TxRecord, CoreError>
    where
        Signer: TxSigner<Intent>,
    {
        if let Some(existing) = self.store.get(&key).await? {
            if !existing.payload.verify_integrity() {
                return Err(CoreError::PayloadIntegrityCorrupted(
                    existing.payload.tx_hash,
                ));
            }
            return Ok(existing);
        }

        let payload = signer.sign_once(intent).await?;
        if !payload.verify_integrity() {
            return Err(CoreError::PayloadIntegrityCorrupted(payload.tx_hash));
        }

        let record = TxRecord {
            idempotency_key: key,
            payload,
            state: TxState::Prepared,
            version: 1,
        };

        self.store.insert_if_absent(record).await
    }

    /// Avança um ciclo da máquina de estados:
    /// 1. Primeiro observa a rede (a transação pode ter entrado após um timeout anterior).
    /// 2. Detecta reorganização se estava `IncludedTentative` e sumiu ou mudou de `block_hash`.
    /// 3. Reenvia estritamente os mesmos `raw_bytes` se ainda não estiver finalizada nem expirada.
    pub async fn step(&self, key: &IdempotencyKey) -> Result<TxRecord, CoreError> {
        let mut record = self
            .store
            .get(key)
            .await?
            .ok_or_else(|| CoreError::Storage(format!("chave não encontrada: {}", key.0)))?;

        if !record.payload.verify_integrity() {
            return Err(CoreError::PayloadIntegrityCorrupted(
                record.payload.tx_hash.clone(),
            ));
        }

        if record.state.is_terminal() {
            return Ok(record);
        }

        // Passo 1: sempre observar antes de reenviar, pois um timeout anterior pode ter sido minerado.
        let obs = self.chain.observe(&record.payload).await?;

        match obs {
            ObservationStatus::Finalized { inclusion } => {
                record.state = TxState::Finalized { inclusion };
                record.version += 1;
                return self.store.compare_and_swap(record).await;
            }
            ObservationStatus::IncludedNotFinalized { inclusion, .. } => {
                match &record.state {
                    TxState::IncludedTentative { inclusion: prev } if prev != &inclusion => {
                        // O bloco mudou na mesma ou em outra altura: houve reorganização e reinclusão em outro bloco.
                        record.state = TxState::IncludedTentative { inclusion };
                        record.version += 1;
                        return self.store.compare_and_swap(record).await;
                    }
                    TxState::IncludedTentative { .. } => {
                        // Continua incluída no mesmo bloco provisório, aguardando finalidade.
                        return Ok(record);
                    }
                    _ => {
                        record.state = TxState::IncludedTentative { inclusion };
                        record.version += 1;
                        return self.store.compare_and_swap(record).await;
                    }
                }
            }
            ObservationStatus::Expired {
                current_block_height,
            } => {
                record.state = TxState::ExpiredOrFailed {
                    reason: format!(
                        "janela de validade expirada na altura {}",
                        current_block_height
                    ),
                };
                record.version += 1;
                return self.store.compare_and_swap(record).await;
            }
            ObservationStatus::Pending { .. } => {
                // Se estava IncludedTentative e voltou a Pending, o bloco sofreu reorg e a tx saiu da cadeia canônica.
                if let TxState::IncludedTentative { inclusion } = &record.state {
                    let prev_reorgs = 1;
                    record.state = TxState::Reorged {
                        previous_inclusion: inclusion.clone(),
                        reorg_count: prev_reorgs,
                    };
                    record.version += 1;
                    record = self.store.compare_and_swap(record).await?;
                }
            }
        }

        // Passo 2: não está incluída nem expirada -> reenviar estritamente os mesmos bytes assinados.
        let attempts = match &record.state {
            TxState::Prepared => 1,
            TxState::SubmittedUnknown { attempts } => attempts + 1,
            TxState::Reorged { .. } => 1,
            TxState::IncludedTentative { .. }
            | TxState::Finalized { .. }
            | TxState::ExpiredOrFailed { .. } => return Ok(record),
        };

        // Persiste a intenção de envio ANTES do disparo para garantir que falha pós-envio fique marcada como Unknown.
        record.state = TxState::SubmittedUnknown { attempts };
        record.version += 1;
        record = self.store.compare_and_swap(record).await?;

        match self.chain.broadcast_raw(&record.payload).await {
            BroadcastOutcome::Accepted | BroadcastOutcome::UnknownTimeout(_) => {
                // Ambos mantêm SubmittedUnknown: timeout é tratado exatamente como resultado desconhecido.
                Ok(record)
            }
            BroadcastOutcome::DefinitiveReject(reason) => {
                // Mesmo em rejeição, verificamos se não houve corrida; se for rejeição definitiva de formato/assinatura:
                record.state = TxState::ExpiredOrFailed { reason };
                record.version += 1;
                self.store.compare_and_swap(record).await
            }
        }
    }
}
