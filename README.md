# tx-finalizer

Biblioteca em Rust para envio idempotente, observação de rede e confirmação com finalidade econômica em blockchains **EVM** e **Solana**.

Projetada para sistemas que movimentam dinheiro on-chain: trata timeouts como resultado desconhecido, reenvia estritamente os mesmos bytes assinados, só confirma após finalidade e reverte estado provisório em caso de reorganização de blocos.

---

## System Design

### 1. Arquitetura de Componentes

O núcleo (`tx-core`) é uma máquina de estados determinística sem dependências de banco ou HTTP. Toda entrada e saída é isolada atrás de três traits (`TxStore`, `TxSigner` e `ChainAdapter`).

```mermaid
flowchart LR
    App[Aplicação / Worker] -->|IdempotencyKey + Intent| Engine[TxEngine<br/>tx-core]
    Engine -->|1. Assina uma única vez| Signer[TxSigner]
    Engine -->|2. INSERT ON CONFLICT / CAS| Store[TxStore<br/>tx-store-pg / InMemoryStore]
    Engine -->|3. Observe antes de enviar<br/>4. Broadcast raw_bytes| Adapter[ChainAdapter]
    Adapter --> EVM[EvmRpcAdapter<br/>tx-evm]
    Adapter --> SOL[SolanaRpcAdapter<br/>tx-solana]
    EVM --> EvmRpc[(EVM JSON-RPC)]
    SOL --> SolRpc[(Solana JSON-RPC)]
    Store --> PG[(PostgreSQL)]
```

### 2. Máquina de Estados

Uma transação nunca é re-assinada enquanto estiver fora de um estado terminal. O `TxEngine::step` sempre executa `observe()` antes de `broadcast_raw()` para detectar inclusões que ocorreram após um timeout anterior.

```mermaid
stateDiagram-v2
    [*] --> Prepared: prepare() persiste SignedPayload

    Prepared --> SubmittedUnknown: broadcast_raw()
    SubmittedUnknown --> SubmittedUnknown: timeout / pending (reenvia mesmos raw_bytes)

    SubmittedUnknown --> IncludedTentative: incluída em bloco não finalizado
    IncludedTentative --> IncludedTentative: mesmo bloco ou novo bloco após reorg (reorg_count++)
    IncludedTentative --> Reorged: bloco órfão removido (volta para Pending)
    Reorged --> SubmittedUnknown: reenvia mesmos raw_bytes

    SubmittedUnknown --> Finalized: bloco <= finalized (EVM) ou finalized (Solana)
    IncludedTentative --> Finalized: atingiu finalidade com sucesso

    SubmittedUnknown --> FinalizedFailed: revert 0x0 (EVM) ou err (Solana) finalizado
    IncludedTentative --> FinalizedFailed: revert 0x0 (EVM) ou err (Solana) finalizado

    SubmittedUnknown --> ExpiredOrFailed: blockHeight > last_valid_block_height ou rejeição definitiva
    Prepared --> ExpiredOrFailed: rejeição definitiva de formato/assinatura

    Finalized --> [*]
    FinalizedFailed --> [*]
    ExpiredOrFailed --> [*]
```

### 3. Invariantes e Modelo de Falhas

| Cenário de Falha | Comportamento da Biblioteca | Garantia |
| :--- | :--- | :--- |
| **Chamadas concorrentes com mesma `IdempotencyKey`** | `insert_if_absent` usa `ON CONFLICT DO NOTHING` no PostgreSQL e retorna o registro vencedor. | No máximo um `SignedPayload` é persistido e enviado por intenção. |
| **Timeout HTTP / queda de conexão no RPC** | `broadcast_raw` retorna `UnknownTimeout`; estado permanece `SubmittedUnknown`. | Nunca gera nova assinatura; próximo ciclo observa a rede e reenvia os mesmos `raw_bytes` (validados por SHA-256). |
| **Reorganização de blocos (Fork/Reorg)** | Na EVM, compara `blockHash` do recibo com o bloco canônico na mesma altura. Na Solana, ignora `processed` e `confirmed`. | Estado provisório (`IncludedTentative`) regride para `Reorged`, incrementa `reorg_count` e não libera efeitos externos. |
| **Revert / Erro de instrução on-chain** | EVM `status == "0x0"` ou Solana `status.err.is_some()` em bloco finalizado transita para `FinalizedFailed`. | Separa falha definitiva on-chain de sucesso (`Finalized`) e de expiração (`ExpiredOrFailed`). |
| **Queda do processo ou limite de polling (`max_steps`)** | `drive_to_finality` devolve o `TxRecord` atual sem alterar para estado terminal. | Outro worker pode retomar a mesma `IdempotencyKey` com segurança via CAS (`WHERE version = $prev`). |
| **Expiração na Solana** | `observe()` compara `getBlockHeight(finalized)` com `last_valid_block_height`. | Só transita para `ExpiredOrFailed` quando a janela expirou deterministicamente. |

---

## Estrutura do Workspace

| Crate | Responsabilidade |
| :--- | :--- |
| `crates/tx-core` | Máquina de estados (`TxEngine`, `TxState`, `SignedPayload`, `PollPolicy`) e traits (`TxStore`, `TxSigner`, `ChainAdapter`). |
| `crates/tx-evm` | Adaptador JSON-RPC EVM (`EvmRpcAdapter`) com checagem de bloco `finalized`, hash canônico e `status == "0x0"`. |
| `crates/tx-solana` | Adaptador JSON-RPC Solana (`SolanaRpcAdapter`) com `commitment: "finalized"` e controle de `last_valid_block_height`. |
| `crates/tx-store-pg` | Persistência PostgreSQL (`PgTxStore`) via `sqlx`, com migração SQL, `raw_bytes` imutáveis e lock otimista por `version`. |
| `crates/tx-sim` | Simulador determinístico em memória (`InMemoryStore` e `ChaosChain`) para testes de caos, concorrência e reorg. |

---

## Como Usar

### 1. Dependências no seu `Cargo.toml`

```toml
[dependencies]
tx-core = { path = "crates/tx-core" }
tx-evm = { path = "crates/tx-evm" }           # ou tx-solana
tx-store-pg = { path = "crates/tx-store-pg" } # ou sua própria implementação de TxStore
async-trait = "0.1"
bytes = "1"
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres"] }
tokio = { version = "1", features = ["full"] }
```

### 2. Fluxo Completo (EVM + PostgreSQL)

```rust
use async_trait::async_trait;
use bytes::Bytes;
use sqlx::PgPool;
use std::time::Duration;
use tx_core::{
    ChainConstraint, CoreError, IdempotencyKey, PollPolicy, SignedPayload, TxEngine, TxSigner,
    TxState,
};
use tx_evm::EvmRpcAdapter;
use tx_store_pg::PgTxStore;

struct PayoutIntent {
    to: String,
    amount_wei: u128,
    nonce: u64,
}

struct MyKmsSigner;

#[async_trait]
impl TxSigner<PayoutIntent> for MyKmsSigner {
    async fn sign_once(&self, intent: &PayoutIntent) -> Result<SignedPayload, CoreError> {
        // Assine aqui via KMS/HSM ou wallet local e retorne os bytes RLP assinados e o tx_hash.
        let raw_rlp = Bytes::from_static(b"\x02\xf8\x01\x02\x03");
        let tx_hash = "0x...".to_string();

        Ok(SignedPayload::new(
            tx_hash,
            raw_rlp,
            ChainConstraint::Evm {
                chain_id: 1,
                nonce: intent.nonce,
            },
        ))
    }
}

async fn execute_payout(pool: PgPool, rpc_url: &str) -> Result<(), CoreError> {
    let store = PgTxStore::new(pool);
    store.migrate().await?;

    let adapter = EvmRpcAdapter::new(rpc_url, Duration::from_secs(10))?;
    let engine = TxEngine::new(store, adapter);

    let key = IdempotencyKey("payout-order-2026-0001".into());
    let intent = PayoutIntent {
        to: "0x000000000000000000000000000000000000dead".into(),
        amount_wei: 1_000_000_000_000_000,
        nonce: 42,
    };

    // 1. Prepara e persiste uma única vez (seguro contra chamadas duplicadas/concorrentes).
    let _record = engine.prepare(key.clone(), &intent, &MyKmsSigner).await?;

    // 2. Avança até finalidade com backoff exponencial.
    let policy = PollPolicy {
        initial_interval: Duration::from_secs(2),
        max_interval: Duration::from_secs(30),
        backoff_multiplier: 2,
        max_steps: 30,
    };

    let result = engine.drive_to_finality(&key, policy).await?;

    match result.state {
        TxState::Finalized { inclusion } => {
            println!("Confirmada em finalidade no bloco {}", inclusion.block_number);
        }
        TxState::FinalizedFailed { reason, .. } => {
            println!("Falhou/reverteu on-chain de forma definitiva: {reason}");
        }
        TxState::ExpiredOrFailed { reason } => {
            println!("Expirou sem entrar na cadeia ({reason}). Seguro gerar nova chave/assinatura.");
        }
        other => {
            // SubmittedUnknown, IncludedTentative ou Reorged:
            // NUNCA re-assine aqui. Agende retomada com a mesma IdempotencyKey.
            println!("Ainda em andamento ({other:?}). Retomar polling com a mesma chave.");
        }
    }

    Ok(())
}
```

Para **Solana**, substitua `EvmRpcAdapter` por `SolanaRpcAdapter::new(rpc_url, Duration::from_secs(10), true)?` e retorne `ChainConstraint::Solana { recent_blockhash, last_valid_block_height }` no `TxSigner`.

---

## Testes e Validação

Rodar toda a suíte unitária e de simulação (concorrência, timeouts, reorgs e mocks HTTP):

```bash
cargo test --workspace
```

Rodar o teste de integração real contra PostgreSQL:

```bash
DATABASE_URL="postgres://postgres:postgres@localhost:5432/tx_finalizer" \
cargo test -p tx-store-pg -- --ignored
```

Checagens executadas no CI (`.github/workflows/ci.yml`):

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
