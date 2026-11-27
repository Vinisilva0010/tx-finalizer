use async_trait::async_trait;
use bytes::Bytes;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use tx_core::{
    ChainConstraint, CoreError, IdempotencyKey, SignedPayload, TxRecord, TxState, TxStore,
};

/// Implementação persistente de `TxStore` sobre PostgreSQL usando `sqlx`.
/// Garante imutabilidade dos bytes assinados após o primeiro `INSERT` e CAS via coluna `version`.
#[derive(Clone)]
pub struct PgTxStore {
    pool: PgPool,
}

impl PgTxStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Aplica as migrações embutidas na pasta `migrations/`.
    pub async fn migrate(&self) -> Result<(), CoreError> {
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .map_err(|e| CoreError::Storage(format!("falha ao aplicar migrações PostgreSQL: {e}")))
    }
}

fn row_to_record(row: PgRow) -> Result<TxRecord, CoreError> {
    let key: String = row
        .try_get("idempotency_key")
        .map_err(|e| CoreError::Storage(e.to_string()))?;
    let tx_hash: String = row
        .try_get("tx_hash")
        .map_err(|e| CoreError::Storage(e.to_string()))?;
    let sha_vec: Vec<u8> = row
        .try_get("payload_sha256")
        .map_err(|e| CoreError::Storage(e.to_string()))?;
    let raw_vec: Vec<u8> = row
        .try_get("raw_bytes")
        .map_err(|e| CoreError::Storage(e.to_string()))?;
    let constraint_val: serde_json::Value = row
        .try_get("constraint_json")
        .map_err(|e| CoreError::Storage(e.to_string()))?;
    let state_val: serde_json::Value = row
        .try_get("state_json")
        .map_err(|e| CoreError::Storage(e.to_string()))?;
    let reorg_count_i32: i32 = row
        .try_get("reorg_count")
        .map_err(|e| CoreError::Storage(e.to_string()))?;
    let version_i64: i64 = row
        .try_get("version")
        .map_err(|e| CoreError::Storage(e.to_string()))?;

    let payload_sha256: [u8; 32] = sha_vec
        .try_into()
        .map_err(|_| CoreError::PayloadIntegrityCorrupted(tx_hash.clone()))?;
    let constraint: ChainConstraint = serde_json::from_value(constraint_val)
        .map_err(|e| CoreError::Storage(format!("parse constraint_json: {e}")))?;
    let state: TxState = serde_json::from_value(state_val)
        .map_err(|e| CoreError::Storage(format!("parse state_json: {e}")))?;
    let reorg_count = u32::try_from(reorg_count_i32)
        .map_err(|_| CoreError::Storage("reorg_count negativo no banco".into()))?;
    let version = u64::try_from(version_i64)
        .map_err(|_| CoreError::Storage("version inválida no banco".into()))?;

    let payload = SignedPayload {
        tx_hash,
        payload_sha256,
        raw_bytes: Bytes::from(raw_vec),
        constraint,
    };

    if !payload.verify_integrity() {
        return Err(CoreError::PayloadIntegrityCorrupted(payload.tx_hash));
    }

    Ok(TxRecord {
        idempotency_key: IdempotencyKey(key),
        payload,
        state,
        reorg_count,
        version,
    })
}

#[async_trait]
impl TxStore for PgTxStore {
    async fn get(&self, key: &IdempotencyKey) -> Result<Option<TxRecord>, CoreError> {
        let maybe_row = sqlx::query(
            r#"
            SELECT idempotency_key, tx_hash, payload_sha256, raw_bytes,
                   constraint_json, state_json, reorg_count, version
            FROM tx_records
            WHERE idempotency_key = $1
            "#,
        )
        .bind(&key.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CoreError::Storage(format!("falha no SELECT tx_records: {e}")))?;

        maybe_row.map(row_to_record).transpose()
    }

    async fn insert_if_absent(&self, record: TxRecord) -> Result<TxRecord, CoreError> {
        if !record.payload.verify_integrity() {
            return Err(CoreError::PayloadIntegrityCorrupted(record.payload.tx_hash));
        }

        let constraint_json = serde_json::to_value(&record.payload.constraint)
            .map_err(|e| CoreError::Storage(format!("serialize constraint_json: {e}")))?;
        let state_json = serde_json::to_value(&record.state)
            .map_err(|e| CoreError::Storage(format!("serialize state_json: {e}")))?;
        let reorg_count = i32::try_from(record.reorg_count)
            .map_err(|_| CoreError::Storage("reorg_count excede i32".into()))?;
        let version = i64::try_from(record.version)
            .map_err(|_| CoreError::Storage("version excede i64".into()))?;

        sqlx::query(
            r#"
            INSERT INTO tx_records (
                idempotency_key, tx_hash, payload_sha256, raw_bytes,
                constraint_json, state_json, reorg_count, version
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (idempotency_key) DO NOTHING
            "#,
        )
        .bind(&record.idempotency_key.0)
        .bind(&record.payload.tx_hash)
        .bind(record.payload.payload_sha256.as_slice())
        .bind(record.payload.raw_bytes.as_ref())
        .bind(constraint_json)
        .bind(state_json)
        .bind(reorg_count)
        .bind(version)
        .execute(&self.pool)
        .await
        .map_err(|e| CoreError::Storage(format!("falha no INSERT tx_records: {e}")))?;

        self.get(&record.idempotency_key)
            .await?
            .ok_or_else(|| CoreError::Storage("registro ausente após insert_if_absent".into()))
    }

    async fn compare_and_swap(&self, record: TxRecord) -> Result<TxRecord, CoreError> {
        if record.version < 2 {
            return Err(CoreError::OptimisticLockConflict {
                expected: record.version,
            });
        }

        let expected_prev_version = i64::try_from(record.version - 1)
            .map_err(|_| CoreError::Storage("version anterior excede i64".into()))?;
        let next_version = i64::try_from(record.version)
            .map_err(|_| CoreError::Storage("version excede i64".into()))?;
        let reorg_count = i32::try_from(record.reorg_count)
            .map_err(|_| CoreError::Storage("reorg_count excede i32".into()))?;
        let state_json = serde_json::to_value(&record.state)
            .map_err(|e| CoreError::Storage(format!("serialize state_json: {e}")))?;

        // Nota de segurança: raw_bytes, tx_hash, payload_sha256 e constraint_json são imutáveis
        // e propositalmente ficam fora da cláusula SET.
        let result = sqlx::query(
            r#"
            UPDATE tx_records
            SET state_json = $1,
                reorg_count = $2,
                version = $3,
                updated_at = NOW()
            WHERE idempotency_key = $4
              AND version = $5
            "#,
        )
        .bind(state_json)
        .bind(reorg_count)
        .bind(next_version)
        .bind(&record.idempotency_key.0)
        .bind(expected_prev_version)
        .execute(&self.pool)
        .await
        .map_err(|e| CoreError::Storage(format!("falha no UPDATE tx_records: {e}")))?;

        if result.rows_affected() == 0 {
            return Err(CoreError::OptimisticLockConflict {
                expected: record.version,
            });
        }

        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requer DATABASE_URL apontando para um PostgreSQL ativo"]
    async fn postgres_store_enforces_idempotency_and_optimistic_locking() {
        let url = std::env::var("DATABASE_URL")
            .expect("defina DATABASE_URL para executar o teste de integração PostgreSQL");
        let pool = PgPool::connect(&url).await.unwrap();
        let store = PgTxStore::new(pool);
        store.migrate().await.unwrap();

        let key = IdempotencyKey(format!(
            "pg-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let payload_a = SignedPayload::new(
            "0xhash_a".into(),
            Bytes::from_static(b"signed_bytes_a"),
            ChainConstraint::Evm {
                chain_id: 1,
                nonce: 1,
            },
        );
        let payload_b = SignedPayload::new(
            "0xhash_b".into(),
            Bytes::from_static(b"signed_bytes_b"),
            ChainConstraint::Evm {
                chain_id: 1,
                nonce: 2,
            },
        );

        let rec_a = TxRecord {
            idempotency_key: key.clone(),
            payload: payload_a.clone(),
            state: TxState::Prepared,
            reorg_count: 0,
            version: 1,
        };
        let rec_b = TxRecord {
            idempotency_key: key.clone(),
            payload: payload_b,
            state: TxState::Prepared,
            reorg_count: 0,
            version: 1,
        };

        let inserted = store.insert_if_absent(rec_a).await.unwrap();
        let duplicate = store.insert_if_absent(rec_b).await.unwrap();
        assert_eq!(inserted.payload.raw_bytes, payload_a.raw_bytes);
        assert_eq!(duplicate.payload.raw_bytes, payload_a.raw_bytes);

        let mut next = inserted.clone();
        next.state = TxState::SubmittedUnknown { attempts: 1 };
        next.version = 2;
        let updated = store.compare_and_swap(next).await.unwrap();
        assert_eq!(updated.version, 2);

        // Tentativa concorrente usando versão obsoleta (2 novamente) deve falhar com OptimisticLockConflict
        let mut stale = inserted;
        stale.state = TxState::SubmittedUnknown { attempts: 99 };
        stale.version = 2;
        let conflict = store.compare_and_swap(stale).await;
        assert!(matches!(
            conflict,
            Err(CoreError::OptimisticLockConflict { .. })
        ));
    }
}
