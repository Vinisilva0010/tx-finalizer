use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;
use tx_core::{
    BlockInclusion, BroadcastOutcome, ChainAdapter, ChainConstraint, CoreError,
    ObservationStatus, SignedPayload,
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
            .timeout(request_timeout)
            .build()
            .map_err(|e| CoreError::Storage(format!("falha ao criar cliente HTTP EVM: {e}")))?;

        Ok(Self {
            rpc_url: rpc_url.into(),
            client,
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
#[serde(rename_all = "camelCase")]
struct EvmReceipt {
    block_number: Option<String>,
    block_hash: Option<String>,
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
        match self.rpc_call("eth_sendRawTransaction", json!([raw_hex])).await {
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

        let latest_block: EvmBlockHeader = serde_json::from_value(
            latest_resp
                .result
                .ok_or_else(|| CoreError::Storage("bloco latest ausente na resposta EVM".into()))?,
        )
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

        let finalized_block: EvmBlockHeader = serde_json::from_value(
            finalized_resp
                .result
                .ok_or_else(|| CoreError::Storage("bloco finalized ausente na resposta EVM".into()))?,
        )
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
                .map_err(|e| CoreError::Storage(format!("falha ao verificar bloco canônico EVM: {e}")))?;

            if let Some(val) = canonical_resp.result
                && !val.is_null()
            {
                let canonical_block: EvmBlockHeader = serde_json::from_value(val)
                    .map_err(|e| CoreError::Storage(format!("parse bloco canônico EVM: {e}")))?;
                if canonical_block.hash.as_deref() == Some(bh.as_str()) {
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
