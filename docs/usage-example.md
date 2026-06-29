# Exemplo de uso — AcmePay (espelhamento entre datacenters)

Exemplo fictício, porém realista, de como configurar e operar o `n-stream-replay`. A
configuração pronta para copiar está em [`examples/acmepay-relay.yaml`](examples/acmepay-relay.yaml).

## Cenário

A **AcmePay** roda Kafka no DC primário em São Paulo e precisa espelhar dois fluxos de eventos
para clusters distintos, cada um com requisitos diferentes de perda/latência:

- **`orders.events`** (origem `orders-sp`) deve ir para:
  - **`dr-rio`** — cluster de _disaster recovery_ no Rio. **Não pode perder nada**, mesmo que o
    link inter-DC fique horas fora.
  - **`analytics`** — cluster de analytics. _Best-effort_: melhor descartar evento antigo do que
    travar o fluxo.
- **`payments.events`** (origem `payments-sp`) deve ir para:
  - **`fraud`** — análise de fraude. Importante, mas tolera duplicatas.
  - **`cold-archive`** — arquivamento de longo prazo. Alto volume, retenção longa.

```
                         ┌──► [fila dr-rio]      ──► Kafka Rio (DR)
orders.events  ──────────┤
   (orders-sp)            └──► [fila analytics]  ──► Kafka Analytics
                         ┌──► [fila fraud]       ──► Kafka Fraude
payments.events ─────────┤
  (payments-sp)           └──► [fila cold-archive] ──► Kafka Arquivamento
```

Cada destino tem **sua própria fila durável em disco**: um cluster offline só faz a fila dele
crescer, sem afetar a origem nem os outros destinos.

## Decisões de configuração por destino

| Destino | Criticidade | `durability` | `onWriteError` | `maxDepth` | `retentionTime` | Por quê |
|---|---|---|---|---|---|---|
| `dr-rio` | Zero perda | `per-record-fsync` | `halt` | 5.000.000 | `0s` (nunca expira) | DR não pode perder: fsync por registro; buffer enorme p/ outage do link; se o disco encher, **para** a origem em vez de descartar. |
| `analytics` | Best-effort | `os-managed` | `drop` | 200.000 | `12h` | Rápido; descarta o mais antigo (contagem/tempo) e nunca trava a origem. |
| `fraud` | Importante | `sync-on-commit` | `drop` | 1.000.000 | `6h` | Durável no commit; tolera duplicatas; retenção média. |
| `cold-archive` | Volume/arquivo | `os-managed` | `drop` | 10.000.000 | `48h` | Alto volume, retenção longa, sem custo de fsync por registro. |

> **Importante (semântica real):** um broker de destino **offline** não é falha de escrita local —
> a origem continua consumindo e enfileirando no disco (store-and-forward), e a fila drena quando o
> broker volta. O `onWriteError` só dispara em **falha de escrita local** (ex.: disco cheio).
> Para `dr-rio`, `halt` significa: se o disco encher, **pare a origem** (prioriza não perder) — o que
> também pausa o destino `analytics` da mesma origem; provisione disco e **monitore `depth`**.
> Já `drop-oldest` (contagem/tempo) é **descarte intencional** do mais antigo não entregue.

## Como executar

```bash
# 1) Pré-requisito (uma vez): publicar a lib de métricas no ~/.m2 local
mvn -f /caminho/nishi-utils-spring/pom.xml clean install

# 2) Build do fat jar
mvn clean package

# 3) Disponibilizar a config ao lado do jar e injetar os segredos por ambiente
mkdir -p config && cp docs/examples/acmepay-relay.yaml config/n-stream-replay.yaml
export NSR_SP_PASSWORD=...   NSR_RIO_PASSWORD=...   NSR_FRAUD_PASSWORD=...

# 4) Subir
./scripts/start.sh
#   ou: java -jar target/n-stream-replay-*.jar --spring.config.additional-location=file:./config/
```

No boot, o engine abre as 4 filas (recuperando backlog do disco, se houver), sobe os _forwarders_
e depois as origens. O log mostra `ReplayEngine iniciado (4 canal(is), 2 origem(ns) ativa(s))`.

## O que observar

Métricas em `GET http://localhost:8080/actuator/prometheus` (séries `nstreamreplay_*`):

| Métrica | O que indica |
|---|---|
| `nstreamreplay_source_orders_sp_consumed_total` | vazão consumida da origem |
| `nstreamreplay_sink_dr_rio_depth` | profundidade da fila do DR (sobe quando o Rio está fora) |
| `nstreamreplay_sink_dr_rio_online` | 1 = entregando; 0 = broker do destino fora |
| `nstreamreplay_sink_analytics_dropped` | descartes por `maxDepth` (**perda silenciosa**) |
| `nstreamreplay_sink_analytics_expired` | descartes por `retentionTime` (**perda silenciosa**) |
| `nstreamreplay_sink_fraud_published` | total entregue ao destino |
| `nstreamreplay_sink_..._poisoned` | registros não-publicáveis descartados (oversize/não-retryable) |

**Cenário de outage do DR:** derrube o `kafka-rio`. Você verá `dr_rio_online` cair para `0` e
`dr_rio_depth` subir, enquanto `orders.raw` (analytics) e a origem seguem normalmente (isolamento).
Ao restabelecer o Rio, a fila `dr-rio` drena sozinha — sem perda, desde que `depth` não tenha
batido o `maxDepth`/disco.

### Alertas Prometheus sugeridos

```promql
# Perda silenciosa: qualquer descarte por contagem ou tempo
increase(nstreamreplay_sink_dropped[5m]) > 0
increase(nstreamreplay_sink_expired[5m]) > 0

# Destino fora por tempo prolongado
max_over_time(nstreamreplay_sink_dr_rio_online[10m]) == 0

# Fila do DR perto do teto (risco de parar a origem por halt)
nstreamreplay_sink_dr_rio_depth > 4500000
```

## Contrato (resumo)

- **At-least-once:** após crash/restart pode haver **duplicatas** no destino. `fraud` e `analytics`
  toleram; para o DR, deduplique a jusante (a proveniência `sourceTopic/partition/offset` vai em
  cada registro) ou trate como idempotente.
- **Sem transformação / sem rate-limit** neste MVP: os bytes de `key`/`value` e os headers são
  repassados intactos; a ordem por chave é preservada (1 forwarder por destino).

Veja a [arquitetura](architecture.md) para os detalhes de corretude e componentes.
