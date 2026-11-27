# tx-finalizer

Biblioteca em Rust para envio idempotente, observação segura e confirmação com finalidade econômica em blockchains **EVM** e **Solana**.

## Garantias e Invariantes

1. **Não-duplicação por chave de idempotência (`IdempotencyKey`):** Uma intenção financeira gera no máximo um `SignedPayload` persistido. Chamadas concorrentes retornam o mesmo registro e os mesmos bytes assinados.
2. **Timeout tratado como resultado desconhecido (`SubmittedUnknown`):** Falhas de rede, timeouts HTTP ou erros ambíguos de RPC nunca disparam nova assinatura. O motor observa a cadeia antes de agir e reenvia exclusivamente os mesmos `raw_bytes` (validados por SHA-256).
3. **Finalidade econômica antes de confirmação:**
   - **EVM:** Exige que o bloco da transação seja menor ou igual ao bloco `finalized` da Beacon Chain e verifica se o `blockHash` canônico nessa altura coincide com o recibo.
   - **Solana:** Ignora `processed` e `confirmed`; só transita para `Finalized` quando `confirmationStatus == "finalized"` sem erro de execução.
4. **Reversão automática em reorganizações (`Reorged`):** Inclusões provisórias ficam em `IncludedTentative`. Se um fork remover o bloco ou alterar o hash naquela altura, o estado regride, incrementa `reorg_count` e retoma o monitoramento/reenvio idêntico.
5. **Expiração determinística:** Na Solana, quando o `blockHeight` finalizado ultrapassa `last_valid_block_height` sem inclusão, a transação transita para `ExpiredOrFailed`, sinalizando que é seguro assinar uma nova tentativa.

## Estrutura do Workspace

- `crates/tx-core`: Máquina de estados determinística, tipos de domínio e traits (`TxStore`, `TxSigner`, `ChainAdapter`).
- `crates/tx-evm`: Adaptador JSON-RPC EVM com detecção de reorg e finalidade via tag `finalized`.
- `crates/tx-solana`: Adaptador JSON-RPC Solana com controle de `last_valid_block_height` e `commitment: "finalized"`.
- `crates/tx-sim`: Simulador determinístico em memória (`InMemoryStore` e `ChaosChain`) para testes de concorrência, timeout e reorg.

## Executar Testes

```bash
cargo test --workspace
