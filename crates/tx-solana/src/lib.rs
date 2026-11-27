use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;
use tx_core::{
    BlockInclusion, BroadcastOutcome, ChainAdapter, ChainConstraint, CoreError,
    ObservationStatus, SignedPayload,
};

/// Adaptador Solana que reenvia os mesmos bytes assinados em Base64 (`skipPreflight: true` opcional
/// para evitar falso-negativo durante reorgs/forks) e só confirma com `commitment: "finalized"`.
pub struct SolanaRpcAdapter {
    rpc_url: String,
    client: Client,
    skip_preflight: bool,
}

impl SolanaRpcAdapter {
    pub fn new(
        rpc_url: impl Into<String>,
        request_timeout: Duration,
        skip_preflight: bool,
    ) -> Result<Self, CoreError> {
        let client = Client::builder()
            .pool_max_idle_per_host(0)
            .timeout(request_timeout)
            .build()
            .map_err(|e| CoreError::Storage(format!("falha ao criar cliente HTTP Solana: {e}")))?;

        Ok(Self {
            rpc_url: rpc_url.into(),
            client,
            skip_preflight,
        })
    }

    async fn rpc_call(&self, method: &str, params: Value) -> Result<JsonRpcResponse, reqwest::Error> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        self.client
            .post(&self.rpc_url)
            .json(&body)
            .send()
            .await?
            .json::<JsonRpcResponse>()
            .await
    }
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponse {
    result: Option<Value>,
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize, Serialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct SignatureStatusesResult {
    value: Vec<Option<SignatureStatus>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SignatureStatus {
    slot: u64,
    confirmation_status: Option<String>,
    err: Option<Value>,
}

fn encode_base64(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;

        out.push(TABLE[((triple >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((triple >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((triple >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(triple & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

#[async_trait]
impl ChainAdapter for SolanaRpcAdapter {
    async fn broadcast_raw(&self, payload: &SignedPayload) -> BroadcastOutcome {
        if !matches!(payload.constraint, ChainConstraint::Solana { .. }) {
            return BroadcastOutcome::DefinitiveReject(
                "payload incompatível com adaptador Solana".into(),
            );
        }

        let b64 = encode_base64(&payload.raw_bytes);
        let config = json!({
            "encoding": "base64",
            "skipPreflight": self.skip_preflight,
            "maxRetries": 0
        });

        match self.rpc_call("sendTransaction", json!([b64, config])).await {
            Ok(resp) => {
                if resp.result.is_some() {
                    return BroadcastOutcome::Accepted;
                }
                if let Some(err) = resp.error {
                    let msg = err.message.to_lowercase();
                    if msg.contains("already processed") {
                        return BroadcastOutcome::Accepted;
                    }
                    if msg.contains("signature verification failure")
                        || msg.contains("invalid transaction")
                    {
                        return BroadcastOutcome::DefinitiveReject(err.message);
                    }
                    // Blockhash not found ou node behind: tratamos como desconhecido para `observe()`
                    // decidir deterministicamente se o blockHeight ultrapassou `last_valid_block_height`.
                    return BroadcastOutcome::UnknownTimeout(format!(
                        "erro RPC Solana ({}): {}",
                        err.code, err.message
                    ));
                }
                BroadcastOutcome::UnknownTimeout("resposta RPC Solana vazia".into())
            }
            Err(e) => BroadcastOutcome::UnknownTimeout(format!("falha de rede/timeout Solana: {e}")),
        }
    }

    async fn observe(&self, payload: &SignedPayload) -> Result<ObservationStatus, CoreError> {
        let last_valid_block_height = match payload.constraint {
            ChainConstraint::Solana {
                last_valid_block_height,
                ..
            } => last_valid_block_height,
            ChainConstraint::Evm { .. } => {
                return Err(CoreError::TerminalChainRejection(
                    "constraint EVM passada ao adaptador Solana".into(),
                ));
            }
        };

        // 1. Consulta status da assinatura com histórico habilitado.
        let status_resp = self
            .rpc_call(
                "getSignatureStatuses",
                json!([[payload.tx_hash], { "searchTransactionHistory": true }]),
            )
            .await
            .map_err(|e| CoreError::Storage(format!("falha ao consultar assinatura Solana: {e}")))?;

        let status_val = status_resp
            .result
            .ok_or_else(|| CoreError::Storage("resultado ausente em getSignatureStatuses".into()))?;

        let parsed: SignatureStatusesResult = serde_json::from_value(status_val)
            .map_err(|e| CoreError::Storage(format!("parse getSignatureStatuses: {e}")))?;

        // 2. Consulta blockHeight finalizado atual da rede para checar expiração do blockhash.
        let height_resp = self
            .rpc_call("getBlockHeight", json!([{ "commitment": "finalized" }]))
            .await
            .map_err(|e| CoreError::Storage(format!("falha ao consultar blockHeight Solana: {e}")))?;

        let current_block_height = height_resp
            .result
            .and_then(|v| v.as_u64())
            .ok_or_else(|| CoreError::Storage("blockHeight inválido na resposta Solana".into()))?;

        if let Some(Some(status)) = parsed.value.into_iter().next() {
            let inclusion = BlockInclusion {
                block_number: status.slot,
                block_hash: format!("slot:{}", status.slot),
            };

            if status.confirmation_status.as_deref() == Some("finalized") {
                if let Some(tx_err) = status.err {
                    return Ok(ObservationStatus::FinalizedFailed {
                        inclusion,
                        reason: format!("transação Solana falhou on-chain: {tx_err}"),
                    });
                }
                return Ok(ObservationStatus::Finalized { inclusion });
            }

            // "processed" ou "confirmed" ainda estão sujeitos a rollback de fork na Solana.
            return Ok(ObservationStatus::IncludedNotFinalized {
                inclusion,
                current_block_height,
            });
        }

        // 3. Não encontrada: verifica se a janela do blockhash já expirou definitivamente.
        if current_block_height > last_valid_block_height {
            return Ok(ObservationStatus::Expired {
                current_block_height,
            });
        }

        Ok(ObservationStatus::Pending {
            current_block_height,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tx_core::{IdempotencyKey, TxEngine, TxRecord, TxState, TxStore};

    #[derive(Clone, Default)]
    struct MemStore(Arc<Mutex<std::collections::HashMap<IdempotencyKey, TxRecord>>>);

    #[async_trait]
    impl TxStore for MemStore {
        async fn get(&self, key: &IdempotencyKey) -> Result<Option<TxRecord>, CoreError> {
            Ok(self.0.lock().unwrap().get(key).cloned())
        }
        async fn insert_if_absent(&self, record: TxRecord) -> Result<TxRecord, CoreError> {
            let mut g = self.0.lock().unwrap();
            if let Some(e) = g.get(&record.idempotency_key) {
                return Ok(e.clone());
            }
            g.insert(record.idempotency_key.clone(), record.clone());
            Ok(record)
        }
        async fn compare_and_swap(&self, record: TxRecord) -> Result<TxRecord, CoreError> {
            let mut g = self.0.lock().unwrap();
            let cur = g.get(&record.idempotency_key).unwrap();
            if cur.version + 1 != record.version {
                return Err(CoreError::OptimisticLockConflict {
                    expected: cur.version + 1,
                });
            }
            g.insert(record.idempotency_key.clone(), record.clone());
            Ok(record)
        }
    }

    async fn spawn_mock_solana_rpc(responses: Vec<String>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let queue = Arc::new(Mutex::new(responses));

        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let body = {
                    let mut g = queue.lock().unwrap();
                    if g.is_empty() {
                        break;
                    }
                    g.remove(0)
                };
                let mut buf = [0u8; 4096];
                let _ = socket.read(&mut buf).await;
                let http = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(http.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });

        format!("http://{addr}")
    }

    #[tokio::test]
    async fn solana_rpc_waits_for_finalized_not_confirmed() {
        let script = vec![
            // Ciclo 1: getSignatureStatuses -> null, getBlockHeight -> 100, sendTransaction -> sig1
            r#"{"jsonrpc":"2.0","id":1,"result":{"value":[null]}}"#.into(),
            r#"{"jsonrpc":"2.0","id":1,"result":100}"#.into(),
            r#"{"jsonrpc":"2.0","id":1,"result":"5sigSolana111"}"#.into(),
            // Ciclo 2: getSignatureStatuses -> confirmed (ainda não finalized!), getBlockHeight -> 105
            r#"{"jsonrpc":"2.0","id":1,"result":{"value":[{"slot":250,"confirmationStatus":"confirmed","err":null}]}}"#.into(),
            r#"{"jsonrpc":"2.0","id":1,"result":105}"#.into(),
            // Ciclo 3: getSignatureStatuses -> finalized, getBlockHeight -> 136
            r#"{"jsonrpc":"2.0","id":1,"result":{"value":[{"slot":250,"confirmationStatus":"finalized","err":null}]}}"#.into(),
            r#"{"jsonrpc":"2.0","id":1,"result":136}"#.into(),
        ];

        let url = spawn_mock_solana_rpc(script).await;
        let adapter = SolanaRpcAdapter::new(url, Duration::from_secs(1), true).unwrap();
        let store = MemStore::default();
        let key = IdempotencyKey("sol-http-1".into());
        let payload = SignedPayload::new(
            "5sigSolana111".into(),
            Bytes::from_static(b"raw_solana_signed_tx_bytes"),
            ChainConstraint::Solana {
                recent_blockhash: "GHtXQBsoZHVnNFa9YevAzFr17DJjgHXk3ycTKD5xD3Zi".into(),
                last_valid_block_height: 250,
            },
        );
        store
            .insert_if_absent(TxRecord {
                idempotency_key: key.clone(),
                payload,
                state: TxState::Prepared,
                reorg_count: 0,
                version: 1,
            })
            .await
            .unwrap();

        let engine = TxEngine::new(store, adapter);

        let r1 = engine.step(&key).await.unwrap();
        assert_eq!(r1.state, TxState::SubmittedUnknown { attempts: 1 });

        // "confirmed" na Solana NÃO libera finalidade -> fica IncludedTentative
        let r2 = engine.step(&key).await.unwrap();
        assert!(matches!(r2.state, TxState::IncludedTentative { .. }));

        // Só quando RPC reporta "finalized" transita para Finalized
        let r3 = engine.step(&key).await.unwrap();
        assert_eq!(
            r3.state,
            TxState::Finalized {
                inclusion: BlockInclusion {
                    block_number: 250,
                    block_hash: "slot:250".into(),
                }
            }
        );
    }

    #[tokio::test]
    async fn solana_rpc_marks_finalized_err_as_finalized_failed() {
        let script = vec![
            r#"{"jsonrpc":"2.0","id":1,"result":{"value":[{"slot":300,"confirmationStatus":"finalized","err":{"InstructionError":[0,{"Custom":1}]}}]}}"#.into(),
            r#"{"jsonrpc":"2.0","id":1,"result":180}"#.into(),
        ];

        let url = spawn_mock_solana_rpc(script).await;
        let adapter = SolanaRpcAdapter::new(url, Duration::from_secs(1), true).unwrap();
        let store = MemStore::default();
        let key = IdempotencyKey("sol-err-1".into());
        let payload = SignedPayload::new(
            "5sigSolanaErr".into(),
            Bytes::from_static(b"raw_solana_err_tx"),
            ChainConstraint::Solana {
                recent_blockhash: "GHtXQBsoZHVnNFa9YevAzFr17DJjgHXk3ycTKD5xD3Zi".into(),
                last_valid_block_height: 250,
            },
        );
        store
            .insert_if_absent(TxRecord {
                idempotency_key: key.clone(),
                payload,
                state: TxState::SubmittedUnknown { attempts: 1 },
                reorg_count: 0,
                version: 1,
            })
            .await
            .unwrap();

        let engine = TxEngine::new(store, adapter);
        let r = engine.step(&key).await.unwrap();
        assert!(matches!(r.state, TxState::FinalizedFailed { .. }));
        assert!(r.state.is_terminal());
    }
}
