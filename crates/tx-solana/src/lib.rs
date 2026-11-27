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
                    return Ok(ObservationStatus::Expired {
                        current_block_height,
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
