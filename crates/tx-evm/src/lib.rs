use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;
use tx_core::{
    BlockInclusion, BroadcastOutcome, ChainAdapter, ChainConstraint, CoreError, ObservationStatus,
    SignedPayload,
};

/// Adaptador EVM que envia estritamente `raw_bytes` via `eth_sendRawTransaction`
/// e só confirma finalidade comparando o recibo com o bloco `finalized` da Beacon Chain.
pub struct EvmRpcAdapter {
    rpc_url: String,
    client: Client,
}

impl EvmRpcAdapter {
    pub fn new(rpc_url: impl Into<String>, request_timeout: Duration) -> Result<Self, CoreError> {
        let client = Client::builder()
            .pool_max_idle_per_host(0)
            .timeout(request_timeout)
            .build()
            .map_err(|e| CoreError::Storage(format!("falha ao criar cliente HTTP EVM: {e}")))?;

        Ok(Self {
            rpc_url: rpc_url.into(),
            client,
        })
    }

    async fn rpc_call(
        &self,
        method: &str,
        params: Value,
    ) -> Result<JsonRpcResponse, reqwest::Error> {
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
#[serde(rename_all = "camelCase")]
struct EvmReceipt {
    block_number: Option<String>,
    block_hash: Option<String>,
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
struct EvmBlockHeader {
    number: Option<String>,
    hash: Option<String>,
}

fn parse_hex_u64(hex_str: &str) -> Result<u64, CoreError> {
    let trimmed = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    u64::from_str_radix(trimmed, 16)
        .map_err(|e| CoreError::Storage(format!("hex inválido '{hex_str}': {e}")))
}

fn encode_hex_prefixed(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("0x");
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

#[async_trait]
impl ChainAdapter for EvmRpcAdapter {
    async fn broadcast_raw(&self, payload: &SignedPayload) -> BroadcastOutcome {
        if !matches!(payload.constraint, ChainConstraint::Evm { .. }) {
            return BroadcastOutcome::DefinitiveReject(
                "payload incompatível com adaptador EVM".into(),
            );
        }

        let raw_hex = encode_hex_prefixed(&payload.raw_bytes);
        match self
            .rpc_call("eth_sendRawTransaction", json!([raw_hex]))
            .await
        {
            Ok(resp) => {
                if resp.result.is_some() {
                    return BroadcastOutcome::Accepted;
                }
                if let Some(err) = resp.error {
                    let msg = err.message.to_lowercase();
                    // "already known" significa que o mesmo payload já está no mempool -> sucesso idempotente.
                    if msg.contains("already known") {
                        return BroadcastOutcome::Accepted;
                    }
                    // "nonce too low" ou "replacement underpriced" pode significar que a própria tx já foi minerada
                    // ou há corrida; tratamos como desconhecido para que `observe()` verifique o recibo na cadeia.
                    if msg.contains("nonce too low") || msg.contains("replacement") {
                        return BroadcastOutcome::UnknownTimeout(format!(
                            "estado ambíguo no mempool EVM: {}",
                            err.message
                        ));
                    }
                    if msg.contains("invalid sender")
                        || msg.contains("rlp")
                        || msg.contains("oversized data")
                        || msg.contains("chain id")
                    {
                        return BroadcastOutcome::DefinitiveReject(err.message);
                    }
                    return BroadcastOutcome::UnknownTimeout(format!(
                        "erro RPC EVM ({}): {}",
                        err.code, err.message
                    ));
                }
                BroadcastOutcome::UnknownTimeout("resposta RPC EVM vazia".into())
            }
            Err(e) => BroadcastOutcome::UnknownTimeout(format!("falha de rede/timeout EVM: {e}")),
        }
    }

    async fn observe(&self, payload: &SignedPayload) -> Result<ObservationStatus, CoreError> {
        // 1. Consulta o último bloco finalizado e o topo atual da cadeia.
        let latest_resp = self
            .rpc_call("eth_getBlockByNumber", json!(["latest", false]))
            .await
            .map_err(|e| CoreError::Storage(format!("falha ao ler bloco latest EVM: {e}")))?;

        let latest_block: EvmBlockHeader =
            serde_json::from_value(latest_resp.result.ok_or_else(|| {
                CoreError::Storage("bloco latest ausente na resposta EVM".into())
            })?)
            .map_err(|e| CoreError::Storage(format!("parse bloco latest EVM: {e}")))?;

        let current_block_height = latest_block
            .number
            .as_deref()
            .map(parse_hex_u64)
            .transpose()?
            .unwrap_or(0);

        let finalized_resp = self
            .rpc_call("eth_getBlockByNumber", json!(["finalized", false]))
            .await
            .map_err(|e| CoreError::Storage(format!("falha ao ler bloco finalized EVM: {e}")))?;

        let finalized_block: EvmBlockHeader =
            serde_json::from_value(finalized_resp.result.ok_or_else(|| {
                CoreError::Storage("bloco finalized ausente na resposta EVM".into())
            })?)
            .map_err(|e| CoreError::Storage(format!("parse bloco finalized EVM: {e}")))?;

        let finalized_height = finalized_block
            .number
            .as_deref()
            .map(parse_hex_u64)
            .transpose()?
            .unwrap_or(0);

        // 2. Consulta o recibo da transação pelo tx_hash.
        let receipt_resp = self
            .rpc_call("eth_getTransactionReceipt", json!([payload.tx_hash]))
            .await
            .map_err(|e| CoreError::Storage(format!("falha ao ler recibo EVM: {e}")))?;

        let Some(receipt_val) = receipt_resp.result else {
            return Ok(ObservationStatus::Pending {
                current_block_height,
            });
        };

        if receipt_val.is_null() {
            return Ok(ObservationStatus::Pending {
                current_block_height,
            });
        }

        let receipt: EvmReceipt = serde_json::from_value(receipt_val)
            .map_err(|e| CoreError::Storage(format!("parse recibo EVM: {e}")))?;

        let (Some(bn_hex), Some(bh)) = (receipt.block_number, receipt.block_hash) else {
            return Ok(ObservationStatus::Pending {
                current_block_height,
            });
        };

        let block_number = parse_hex_u64(&bn_hex)?;
        let inclusion = BlockInclusion {
            block_number,
            block_hash: bh.clone(),
        };

        // 3. Só considera finalizada se a altura do bloco <= altura do bloco `finalized`
        // E o hash do bloco na cadeia canônica nessa altura ainda coincidir com o recibo.
        if block_number <= finalized_height {
            let canonical_resp = self
                .rpc_call("eth_getBlockByNumber", json!([bn_hex, false]))
                .await
                .map_err(|e| {
                    CoreError::Storage(format!("falha ao verificar bloco canônico EVM: {e}"))
                })?;

            if let Some(val) = canonical_resp.result
                && !val.is_null()
            {
                let canonical_block: EvmBlockHeader = serde_json::from_value(val)
                    .map_err(|e| CoreError::Storage(format!("parse bloco canônico EVM: {e}")))?;
                if canonical_block.hash.as_deref() == Some(bh.as_str()) {
                    if receipt.status.as_deref() == Some("0x0") {
                        return Ok(ObservationStatus::FinalizedFailed {
                            inclusion,
                            reason: "transação EVM revertida on-chain (status 0x0)".into(),
                        });
                    }
                    return Ok(ObservationStatus::Finalized { inclusion });
                }
            }
            // Se o hash divergente foi detectado, houve reorg e o recibo ficou órfão.
            return Ok(ObservationStatus::Pending {
                current_block_height,
            });
        }

        Ok(ObservationStatus::IncludedNotFinalized {
            inclusion,
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

    async fn spawn_mock_evm_rpc(responses: Vec<(u16, String, u64)>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let queue = Arc::new(Mutex::new(responses));

        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let next = {
                    let mut g = queue.lock().unwrap();
                    if g.is_empty() {
                        break;
                    }
                    g.remove(0)
                };
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let _ = socket.read(&mut buf).await;
                    let (status, body, delay_ms) = next;
                    if delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                    let http = format!(
                        "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(http.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        });

        format!("http://{addr}")
    }

    #[tokio::test]
    async fn evm_rpc_handles_timeout_reorg_and_finality() {
        // Sequência de chamadas RPC feitas pelos 3 ciclos de `engine.step`:
        // Ciclo 1:
        //   1. eth_getBlockByNumber("latest") -> 0x10 (16)
        //   2. eth_getBlockByNumber("finalized") -> 0x0a (10)
        //   3. eth_getTransactionReceipt -> null
        //   4. eth_sendRawTransaction -> delay de 250ms (maior que timeout de 80ms -> UnknownTimeout)
        // Ciclo 2:
        //   5. eth_getBlockByNumber("latest") -> 0x11 (17)
        //   6. eth_getBlockByNumber("finalized") -> 0x0c (12)
        //   7. eth_getTransactionReceipt -> incluído no bloco 0x11 com hash 0xorphan
        // Ciclo 3:
        //   8. eth_getBlockByNumber("latest") -> 0x15 (21)
        //   9. eth_getBlockByNumber("finalized") -> 0x12 (18)
        //  10. eth_getTransactionReceipt -> reincluído no bloco 0x12 com hash 0xcanonical
        //  11. eth_getBlockByNumber("0x12") -> hash 0xcanonical (confirma que não houve reorg nesse bloco)
        let script = vec![
            (200, r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x10","hash":"0x10"}}"#.into(), 0),
            (200, r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x0a","hash":"0x0a"}}"#.into(), 0),
            (200, r#"{"jsonrpc":"2.0","id":1,"result":null}"#.into(), 0),
            (200, r#"{"jsonrpc":"2.0","id":1,"result":"0xtx1"}"#.into(), 250),
            (200, r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x11","hash":"0x11"}}"#.into(), 0),
            (200, r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x0c","hash":"0x0c"}}"#.into(), 0),
            (200, r#"{"jsonrpc":"2.0","id":1,"result":{"blockNumber":"0x11","blockHash":"0xorphan"}}"#.into(), 0),
            (200, r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x15","hash":"0x15"}}"#.into(), 0),
            (200, r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x12","hash":"0x12"}}"#.into(), 0),
            (200, r#"{"jsonrpc":"2.0","id":1,"result":{"blockNumber":"0x12","blockHash":"0xcanonical"}}"#.into(), 0),
            (200, r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x12","hash":"0xcanonical"}}"#.into(), 0),
        ];

        let url = spawn_mock_evm_rpc(script).await;
        let adapter = EvmRpcAdapter::new(url, Duration::from_millis(80)).unwrap();
        let store = MemStore::default();
        let key = IdempotencyKey("evm-http-1".into());
        let payload = SignedPayload::new(
            "0xtx1".into(),
            Bytes::from_static(b"\x02\xf8\x01\x02\x03"),
            ChainConstraint::Evm {
                chain_id: 1,
                nonce: 5,
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

        // Ciclo 1: timeout tratado como SubmittedUnknown
        let r1 = engine.step(&key).await.unwrap();
        assert_eq!(r1.state, TxState::SubmittedUnknown { attempts: 1 });

        // Ciclo 2: incluída em bloco provisório (0x11 > finalized 0x0c)
        let r2 = engine.step(&key).await.unwrap();
        assert_eq!(
            r2.state,
            TxState::IncludedTentative {
                inclusion: BlockInclusion {
                    block_number: 17,
                    block_hash: "0xorphan".into(),
                }
            }
        );

        // Ciclo 3: bloco 0x12 <= finalized 0x12 e hash canônico confere -> Finalized (reorg_count == 1)
        let r3 = engine.step(&key).await.unwrap();
        assert_eq!(r3.reorg_count, 1);
        assert_eq!(
            r3.state,
            TxState::Finalized {
                inclusion: BlockInclusion {
                    block_number: 18,
                    block_hash: "0xcanonical".into(),
                }
            }
        );
    }

    #[tokio::test]
    async fn evm_rpc_marks_reverted_finalized_tx_as_finalized_failed() {
        let script = vec![
            (200, r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x20","hash":"0x20"}}"#.into(), 0),
            (200, r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x18","hash":"0x18"}}"#.into(), 0),
            (200, r#"{"jsonrpc":"2.0","id":1,"result":{"blockNumber":"0x15","blockHash":"0xreverted_block","status":"0x0"}}"#.into(), 0),
            (200, r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x15","hash":"0xreverted_block"}}"#.into(), 0),
        ];

        let url = spawn_mock_evm_rpc(script).await;
        let adapter = EvmRpcAdapter::new(url, Duration::from_secs(1)).unwrap();
        let store = MemStore::default();
        let key = IdempotencyKey("evm-revert-1".into());
        let payload = SignedPayload::new(
            "0xtx_revert".into(),
            Bytes::from_static(b"\x02\xf8\x01\x02\x03"),
            ChainConstraint::Evm {
                chain_id: 1,
                nonce: 9,
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
