# n-stream-replay

Relay **Kafka store-and-forward** com **roteamento por tópico**: declaram-se **origens** (sources) e **destinos** (sinks) como conexões puras, e **rotas** que mapeiam um **tópico de origem → N alvos `{sink, toTopic?, partitionKey?}`** (de-para, fan-out, merge, espelho). O payload é preservado; a chave de publicação pode ser extraída de JSON ou YAML.

Cada **destino** possui uma **fila durável (NQueue, em disco)** própria. Se o destino fica offline, as mensagens dele ficam na fila e são **re-entregues quando ele volta** — sem travar a origem nem os demais destinos.

```
                       ┌─ rota: orders.in → {dc2-mirror, toTopic=orders.mirror}
orders.in (orders-src) ┤
                       └─ rota: returns.in → {analytics}            (toTopic ausente = preserva)
   origem (consome a união dos fromTopic) ──► fila-NQueue (por destino) ──► destino (no tópico da rota)
```

- **de-para:** tópico A → sink X, tópico B → sink Y (rotas distintas).
- **fan-out:** uma rota com vários `to` (1 tópico → N destinos).
- **merge:** duas rotas com o mesmo `{sink, toTopic}` (N tópicos → 1 destino).
- **espelho:** `toTopic` omitido → o destino preserva o nome do tópico de origem.
- **rate limit por origem:** `maxConsumeRatePerSec` (registros/s; `0` = ilimitado) limita a taxa de consumo da origem.

## Decoder e chave de publicação

Por padrão, `decoder: text` mantém o encaminhamento dos bytes originais. Para definir
uma chave Kafka a partir do payload, configure `decoder: json` ou `decoder: yaml` na
rota e `partitionKey` no alvo da rota:

```yaml
nstreamreplay:
  sources:
    - id: orders-src
      bootstrapServers: "localhost:9092"
      groupId: orders-relay
  sinks:
    - id: dc2-mirror
      bootstrapServers: "localhost:9092"
  routes:
    - id: orders-mirror
      source: orders-src
      fromTopic: orders.in
      decoder: json
      to:
        - { sink: dc2-mirror, toTopic: orders.mirror, partitionKey: "payload.Id" }
```

A precedência é **decoder da rota → decoder do source → `text`**. O campo no source
continua aceito como padrão de compatibilidade com a v2.2.0. Assim, um único source
pode consumir tópicos JSON e YAML, cada um com seu decoder na rota. O decode é
compartilhado por formato dentro da mesma mensagem; uma falha em JSON não impede
uma rota YAML de processá-la. `decodeErrors` conta a mensagem uma única vez mesmo
quando mais de um formato falha. Alvos idênticos com chave configurada e decoders
efetivos diferentes são considerados conflitantes e impedem o boot.

`payload.Id` acessa uma propriedade aninhada; `id` acessa a raiz. Os nomes são
sensíveis a maiúsculas. Cada segmento aceita letras, números, `_`, `$` e `-`;
o ponto separa propriedades. Não há indexação de arrays, curingas, expressões nem
escape para nomes contendo ponto.

- Strings (inclusive vazias), números e booleanos viram a chave em UTF-8, sem aspas
  JSON e sem remover espaços. A chave original é substituída apenas nesse alvo.
- Campo ausente/null, objeto/array ou payload inválido descartam **somente os alvos
  que exigem a chave**. Tombstones são tratados como chave ausente. A aplicação
  continua e confirma os offsets após processar/sincronizar os demais alvos.
- Sem `partitionKey`, a chave e o payload originais são preservados, mesmo se o
  payload não puder ser decodificado. O decoder é executado somente quando necessário.
- O payload nunca é reserializado. A chave extraída é gravada na fila durável antes
  do commit e permanece igual após retries, reinícios ou mudança de configuração.
- Decoder desconhecido, `custom` (reservado para o futuro), caminho inválido,
  `partitionKey` com `text` e políticas conflitantes para um mesmo destino impedem
  o boot. JSON/YAML devem conter um único documento; propriedades duplicadas são inválidas.

O Kafka escolhe a partição pela chave usando o producer configurado. O recurso serve
para mensagens sem chave em uma origem de uma partição e também para outras topologias.
As opções Kafka em `extraProps` continuam sob responsabilidade da configuração do producer.

Os stats incluem `decodeErrors` (por mensagem), `decodeDropped`, `keyMissingDropped`
e `keyTypeDropped` (por alvo deduplicado). Também são expostos por source no Prometheus;
veja os nomes completos na [arquitetura](docs/architecture.md). São contadores da execução
atual; reprocessamentos após falha de commit podem contabilizar novamente o descarte.

## Stack
- Java 25, Spring Boot 3.5.6 (web servlet, apenas para expor métricas)
- `kafka-clients` 3.9.1 (consumers/producers crus — sem Spring Kafka)
- `dev.nishisan:nishi-utils-core:8.1.0` — `NQueue` (fila durável) + `StatsUtils` (métricas)
- `dev.nishisan:nishi-utils-spring:2.1.1` — ponte `StatsUtils → Micrometer → /actuator/prometheus`

O cliente Kafka 3.9.1 corrige o uso de `Subject.getSubject()` na autenticação
SASL/SCRAM com Java 25. Não utilize o cliente 3.9.0 nessa combinação: ele pode falhar
com `UnsupportedOperationException: getSubject is not supported`. O teste
`ScramJavaCompatibilityTest` cobre a leitura das credenciais e o início do handshake.

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

- Configuração de app: [`config/n-stream-replay.yaml`](config/n-stream-replay.yaml) (sources/sinks/routes).
- Métricas: `GET http://localhost:8080/actuator/prometheus` (séries `nstreamreplay_*`).
- Health: `GET http://localhost:8080/actuator/health`.

## Garantias e contrato
- **At-least-once.** Após crash/restart pode haver **duplicatas** no destino — sinks devem tolerar redelivery. Não há dedup nem exactly-once no MVP.
- **Durabilidade `sync-on-commit` (default):** o offset da origem só é commitado depois que o lote foi gravado e sincronizado (`fsync`) em todas as filas-destino.
- **Bounded drop-oldest:** cada fila é limitada por `maxDepth` (contagem) e/ou `retentionTime` (tempo); ao estourar, os registros **mais antigos não entregues são descartados** (perda intencional, contabilizada em `dropped`/`expired`).

## Documentação
- [Arquitetura](docs/architecture.md) — componentes, garantias de corretude, métricas.
- [Exemplo de uso (AcmePay)](docs/usage-example.md) — cenário real de espelhamento entre datacenters, com [config pronta](docs/examples/acmepay-relay.yaml).
