# n-stream-replay

Relay **Kafka store-and-forward**: declaram-se **origens** (sources) e **destinos** (sinks) Kafka, e **pipelines** que amarram **1 origem → N destinos**. Sem transformação de mensagem e sem rate-limit (MVP).

Cada **destino** possui uma **fila durável (NQueue, em disco)** própria. Se o destino fica offline, as mensagens dele ficam na fila e são **re-entregues quando ele volta** — sem travar a origem nem os demais destinos.

```
origem  ──►  pipeline (resolve destinos)  ──►  fila-NQueue (por destino)  ──►  destino
                                          └──►  fila-NQueue (por destino)  ──►  destino
```

## Stack
- Java 25, Spring Boot 3.5.6 (web servlet, apenas para expor métricas)
- `kafka-clients` 3.9.0 (consumers/producers crus — sem Spring Kafka)
- `dev.nishisan:nishi-utils-core:8.1.0` — `NQueue` (fila durável) + `StatsUtils` (métricas)
- `dev.nishisan:nishi-utils-spring:2.1.1` — ponte `StatsUtils → Micrometer → /actuator/prometheus`

## Build

> **Pré-requisito (uma vez):** publicar `nishi-utils-spring:2.1.1` no `~/.m2` local:
> ```bash
> mvn -f /home/lucas/Projects/nishisan/nishi-utils-spring/pom.xml clean install
> ```

```bash
mvn clean package          # gera o fat jar executável em target/n-stream-replay-<versão>.jar
```

## Execução
```bash
./scripts/start.sh         # usa target/n-stream-replay-*.jar + ./config/
# ou
java -jar target/n-stream-replay-*.jar --spring.config.additional-location=file:./config/
```

- Configuração de app: [`config/n-stream-replay.yaml`](config/n-stream-replay.yaml) (sources/sinks/pipelines).
- Métricas: `GET http://localhost:8080/actuator/prometheus` (séries `nstreamreplay_*`).
- Health: `GET http://localhost:8080/actuator/health`.

## Garantias e contrato
- **At-least-once.** Após crash/restart pode haver **duplicatas** no destino — sinks devem tolerar redelivery. Não há dedup nem exactly-once no MVP.
- **Durabilidade `sync-on-commit` (default):** o offset da origem só é commitado depois que o lote foi gravado e sincronizado (`fsync`) em todas as filas-destino.
- **Bounded drop-oldest:** cada fila é limitada por `maxDepth` (contagem) e/ou `retentionTime` (tempo); ao estourar, os registros **mais antigos não entregues são descartados** (perda intencional, contabilizada em `dropped`/`expired`).

## Documentação
- [Arquitetura](docs/architecture.md) — componentes, garantias de corretude, métricas.
- [Exemplo de uso (AcmePay)](docs/usage-example.md) — cenário real de espelhamento entre datacenters, com [config pronta](docs/examples/acmepay-relay.yaml).
