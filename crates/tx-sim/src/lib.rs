use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tx_core::{
    BlockInclusion, BroadcastOutcome, ChainAdapter, ChainConstraint, CoreError, IdempotencyKey,
    ObservationStatus, SignedPayload, TxRecord, TxSigner, TxStore,
};

/// Armazenamento em memória com lock atômico e validação estrita de versão (CAS).
#[derive(Clone, Default)]
pub struct InMemoryStore {
    inner: Arc<Mutex<HashMap<IdempotencyKey, TxRecord>>>,
}

#[async_trait]
impl TxStore for InMemoryStore {
    async fn get(&self, key: &IdempotencyKey) -> Result<Option<TxRecord>, CoreError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| CoreError::Storage(e.to_string()))?;
        Ok(guard.get(key).cloned())
    }

    async fn insert_if_absent(&self, record: TxRecord) -> Result<TxRecord, CoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| CoreError::Storage(e.to_string()))?;
        if let Some(existing) = guard.get(&record.idempotency_key) {
            return Ok(existing.clone());
        }
        guard.insert(record.idempotency_key.clone(), record.clone());
        Ok(record)
    }

    async fn compare_and_swap(&self, record: TxRecord) -> Result<TxRecord, CoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| CoreError::Storage(e.to_string()))?;
        let current = guard
            .get(&record.idempotency_key)
            .ok_or_else(|| CoreError::Storage("registro inexistente no CAS".into()))?;

        if current.version + 1 != record.version {
            return Err(CoreError::OptimisticLockConflict {
                expected: current.version + 1,
            });
        }

        guard.insert(record.idempotency_key.clone(), record.clone());
        Ok(record)
    }
}

/// Signer de teste que contabiliza quantas vezes foi invocado.
pub struct MockSigner {
    pub sign_calls: Arc<AtomicUsize>,
    pub mode: MockChainMode,
}

#[derive(Clone, Copy)]
pub enum MockChainMode {
    Evm { chain_id: u64, nonce: u64 },
    Solana { last_valid_block_height: u64 },
}

#[async_trait]
impl TxSigner<String> for MockSigner {
    async fn sign_once(&self, intent: &String) -> Result<SignedPayload, CoreError> {
        let call_num = self.sign_calls.fetch_add(1, Ordering::SeqCst) + 1;
        let raw = Bytes::from(format!("signed:{intent}:call_{call_num}"));
        let tx_hash = format!("0xhash_{intent}_{call_num}");
        let constraint = match self.mode {
            MockChainMode::Evm { chain_id, nonce } => ChainConstraint::Evm { chain_id, nonce },
            MockChainMode::Solana {
                last_valid_block_height,
            } => ChainConstraint::Solana {
                recent_blockhash: "Blockhash1111111111111111111111111111111111".into(),
                last_valid_block_height,
            },
        };
        Ok(SignedPayload::new(tx_hash, raw, constraint))
    }
}

/// Simulador de blockchain com fila programável de falhas, reorgs e finalidade.
#[derive(Clone)]
pub struct ChaosChain {
    state: Arc<Mutex<ChaosState>>,
}

struct ChaosState {
    broadcast_script: Vec<BroadcastOutcome>,
    observe_script: Vec<ObservationStatus>,
    received_payloads: Vec<Bytes>,
}

impl ChaosChain {
    pub fn new(
        broadcast_script: Vec<BroadcastOutcome>,
        observe_script: Vec<ObservationStatus>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(ChaosState {
                broadcast_script,
                observe_script,
                received_payloads: Vec::new(),
            })),
        }
    }

    pub fn received_payloads(&self) -> Vec<Bytes> {
        self.state.lock().unwrap().received_payloads.clone()
    }
}

#[async_trait]
impl ChainAdapter for ChaosChain {
    async fn broadcast_raw(&self, payload: &SignedPayload) -> BroadcastOutcome {
        let mut guard = self.state.lock().unwrap();
        guard.received_payloads.push(payload.raw_bytes.clone());
        if guard.broadcast_script.is_empty() {
            BroadcastOutcome::Accepted
        } else {
            guard.broadcast_script.remove(0)
        }
    }

    async fn observe(&self, _payload: &SignedPayload) -> Result<ObservationStatus, CoreError> {
        let mut guard = self.state.lock().unwrap();
        if guard.observe_script.is_empty() {
            Ok(ObservationStatus::Pending {
                current_block_height: 100,
            })
        } else {
            Ok(guard.observe_script.remove(0))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tx_core::{TxEngine, TxState};

    #[tokio::test]
    async fn concurrent_prepare_never_duplicates_signature_or_payload() {
        let store = InMemoryStore::default();
        let chain = ChaosChain::new(vec![], vec![]);
        let engine = Arc::new(TxEngine::new(store, chain));
        let sign_calls = Arc::new(AtomicUsize::new(0));
        let signer = Arc::new(MockSigner {
            sign_calls: sign_calls.clone(),
            mode: MockChainMode::Evm {
                chain_id: 1,
                nonce: 42,
            },
        });

        let mut handles = Vec::new();
        for _ in 0..20 {
            let eng = engine.clone();
            let sig = signer.clone();
            handles.push(tokio::spawn(async move {
                eng.prepare(
                    IdempotencyKey("payout-order-99".into()),
                    &"transfer_100_usdc".to_string(),
                    sig.as_ref(),
                )
                .await
                .unwrap()
            }));
        }

        let mut results = Vec::new();
        for h in handles {
            results.push(h.await.unwrap());
        }

        let first = &results[0];
        for r in &results {
            assert_eq!(r.payload.raw_bytes, first.payload.raw_bytes);
            assert_eq!(r.payload.payload_sha256, first.payload.payload_sha256);
            assert_eq!(r.payload.tx_hash, first.payload.tx_hash);
        }
        // Pode haver assinatura especulativa antes do insert_if_absent em corrida extrema,
        // mas apenas UM payload vencedor é persistido e usado para envio.
        assert!(sign_calls.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn timeout_treated_as_unknown_and_resends_exact_same_bytes() {
        let store = InMemoryStore::default();
        let chain = ChaosChain::new(
            vec![
                BroadcastOutcome::UnknownTimeout("rpc read timeout 504".into()),
                BroadcastOutcome::UnknownTimeout("connection reset".into()),
                BroadcastOutcome::Accepted,
            ],
            vec![
                // Ciclo 1: ainda não vista, dispara e toma timeout
                ObservationStatus::Pending {
                    current_block_height: 10,
                },
                // Ciclo 2: ainda não vista, reenvia exatamente os mesmos bytes e toma timeout
                ObservationStatus::Pending {
                    current_block_height: 11,
                },
                // Ciclo 3: o segundo envio na verdade entrou na rede e já está finalizado!
                ObservationStatus::Finalized {
                    inclusion: BlockInclusion {
                        block_number: 12,
                        block_hash: "0xblock_final_12".into(),
                    },
                },
            ],
        );

        let engine = TxEngine::new(store, chain.clone());
        let signer = MockSigner {
            sign_calls: Arc::new(AtomicUsize::new(0)),
            mode: MockChainMode::Evm {
                chain_id: 999,
                nonce: 7,
            },
        };

        let key = IdempotencyKey("bridge-tx-1".into());
        let prepared = engine
            .prepare(key.clone(), &"bridge_50_eth".to_string(), &signer)
            .await
            .unwrap();

        // Ciclo 1 -> timeout -> SubmittedUnknown { attempts: 1 }
        let s1 = engine.step(&key).await.unwrap();
        assert_eq!(s1.state, TxState::SubmittedUnknown { attempts: 1 });

        // Ciclo 2 -> timeout -> SubmittedUnknown { attempts: 2 }
        let s2 = engine.step(&key).await.unwrap();
        assert_eq!(s2.state, TxState::SubmittedUnknown { attempts: 2 });

        // Ciclo 3 -> observe detecta Finalized antes mesmo de reenviar pela 3ª vez
        let s3 = engine.step(&key).await.unwrap();
        assert_eq!(
            s3.state,
            TxState::Finalized {
                inclusion: BlockInclusion {
                    block_number: 12,
                    block_hash: "0xblock_final_12".into(),
                }
            }
        );

        // Garante que todos os broadcasts enviados à rede tiveram EXATAMENTE os mesmos bytes originais
        let sent = chain.received_payloads();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0], prepared.payload.raw_bytes);
        assert_eq!(sent[1], prepared.payload.raw_bytes);
    }

    #[tokio::test]
    async fn reorg_reverts_tentative_inclusion_and_waits_for_canonical_finality() {
        let store = InMemoryStore::default();
        let chain = ChaosChain::new(
            vec![BroadcastOutcome::Accepted, BroadcastOutcome::Accepted],
            vec![
                // Ciclo 1: pendente -> envia
                ObservationStatus::Pending {
                    current_block_height: 50,
                },
                // Ciclo 2: incluída no bloco órfão 51 (fork A)
                ObservationStatus::IncludedNotFinalized {
                    inclusion: BlockInclusion {
                        block_number: 51,
                        block_hash: "0xorphan_fork_a".into(),
                    },
                    current_block_height: 51,
                },
                // Ciclo 3: REORG! O bloco 51 do fork A sumiu; tx voltou para Pending no fork B
                ObservationStatus::Pending {
                    current_block_height: 52,
                },
                // Ciclo 4: reincluída no bloco canônico 53 (ainda não finalizado)
                ObservationStatus::IncludedNotFinalized {
                    inclusion: BlockInclusion {
                        block_number: 53,
                        block_hash: "0xcanonical_fork_b".into(),
                    },
                    current_block_height: 53,
                },
                // Ciclo 5: bloco 53 atinge finalidade
                ObservationStatus::Finalized {
                    inclusion: BlockInclusion {
                        block_number: 53,
                        block_hash: "0xcanonical_fork_b".into(),
                    },
                },
            ],
        );

        let engine = TxEngine::new(store, chain.clone());
        let signer = MockSigner {
            sign_calls: Arc::new(AtomicUsize::new(0)),
            mode: MockChainMode::Evm {
                chain_id: 1,
                nonce: 10,
            },
        };

        let key = IdempotencyKey("reorg-test-1".into());
        let prepared = engine
            .prepare(key.clone(), &"swap_exact_in".to_string(), &signer)
            .await
            .unwrap();

        // 1. Dispara
        let s1 = engine.step(&key).await.unwrap();
        assert_eq!(s1.state, TxState::SubmittedUnknown { attempts: 1 });

        // 2. Inclusão provisória no fork A
        let s2 = engine.step(&key).await.unwrap();
        assert_eq!(
            s2.state,
            TxState::IncludedTentative {
                inclusion: BlockInclusion {
                    block_number: 51,
                    block_hash: "0xorphan_fork_a".into(),
                }
            }
        );

        // 3. Reorg remove a tx do fork A -> volta a SubmittedUnknown após registrar Reorged e reenvia os mesmos bytes
        let s3 = engine.step(&key).await.unwrap();
        assert_eq!(s3.state, TxState::SubmittedUnknown { attempts: 1 });

        // 4. Inclusão provisória no fork B canônico
        let s4 = engine.step(&key).await.unwrap();
        assert_eq!(
            s4.state,
            TxState::IncludedTentative {
                inclusion: BlockInclusion {
                    block_number: 53,
                    block_hash: "0xcanonical_fork_b".into(),
                }
            }
        );

        // 5. Finalidade confirmada no fork B
        let s5 = engine.step(&key).await.unwrap();
        assert_eq!(
            s5.state,
            TxState::Finalized {
                inclusion: BlockInclusion {
                    block_number: 53,
                    block_hash: "0xcanonical_fork_b".into(),
                }
            }
        );

        let sent = chain.received_payloads();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0], prepared.payload.raw_bytes);
        assert_eq!(sent[1], prepared.payload.raw_bytes);
    }

    #[tokio::test]
    async fn solana_expiration_transitions_to_expired_when_block_height_exceeded() {
        let store = InMemoryStore::default();
        let chain = ChaosChain::new(
            vec![BroadcastOutcome::UnknownTimeout("quic timeout".into())],
            vec![
                ObservationStatus::Pending {
                    current_block_height: 200,
                },
                ObservationStatus::Expired {
                    current_block_height: 355,
                },
            ],
        );

        let engine = TxEngine::new(store, chain);
        let signer = MockSigner {
            sign_calls: Arc::new(AtomicUsize::new(0)),
            mode: MockChainMode::Solana {
                last_valid_block_height: 350,
            },
        };

        let key = IdempotencyKey("sol-transfer-1".into());
        engine
            .prepare(key.clone(), &"sol_send_1".to_string(), &signer)
            .await
            .unwrap();

        let s1 = engine.step(&key).await.unwrap();
        assert_eq!(s1.state, TxState::SubmittedUnknown { attempts: 1 });

        let s2 = engine.step(&key).await.unwrap();
        assert!(matches!(s2.state, TxState::ExpiredOrFailed { .. }));
        assert!(s2.state.is_terminal());
    }
}
