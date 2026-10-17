[English documentation](README.md) · [Compare with Apache Kafka MirrorMaker 2](docs/mirrormaker-vs-n-stream-replay.md)

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
- **partições preservadas:** `partitioning: preserve` no alvo mantém a partição (P da origem → P do destino); veja abaixo.
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

## Preservar a partição de origem (`partitioning: preserve`)

Por padrão (`partitioning: key`) o producer do destino escolhe a partição pela chave do
registro. Isso quebra quem depende da **partição**: por exemplo, um tópico de CDC cujos
marcadores de controle (`reset`, `snapshot_end`, `resume`, `retain`) são publicados **uma vez
em cada partição, todos com a mesma chave**, e cujo consumidor reconstrói o estado por
partição. Reparticionar pela chave juntaria as cópias do marcador numa partição só.

```yaml
routes:
  - id: alarms-cdc-mirror
    source: prod-cdc
    fromTopic: fault.alarms.cdc
    to:
      - { sink: dev-cdc-mirror, partitioning: preserve }   # toTopic opcional
```

- O registro da partição **P** da origem vai para a partição **P** do destino, com a chave em
  bytes crus, os headers, o valor e o timestamp intactos. A ordem dentro de cada partição é
  mantida (FIFO por destino; producer com `acks=all` idempotente, ou `max.in.flight=1`
  quando não há idempotência, para o retry não reordenar).
- **Pré-requisito:** o tópico de destino já existe e tem **pelo menos tantas partições quanto
  o de origem**. A aplicação **não cria tópicos**; na subida ela consulta origem e destino
  (`Admin`) e **não sobe** se o destino não existir, tiver menos partições ou se algum cluster
  não puder ser consultado. A mensagem nomeia rota, tópicos e contagens. Rotas sem `preserve`
  não abrem conexão administrativa.
- `preserve` com `partitionKey` no mesmo alvo é erro de configuração. Alvos idênticos
  (mesma origem/tópico/sink/destino) com `partitioning` diferente também impedem o boot.
- A partição de destino é gravada junto com o registro na fila durável. **Compatibilidade:**
  filas gravadas pela v2.3.x continuam legíveis após o upgrade e os registros antigos são
  publicados como antes (sem partição fixa, pela chave). Voltar para a v2.3.x com uma fila
  já escrita pela 2.4.0 ignora a partição gravada.
- Se o destino ganhar uma partição inexistente (ex.: a origem foi ampliada depois), o envio
  daquele registro falha de forma transitória: o registro **fica na fila** (não é descartado)
  e é retentado; aumente as partições do destino. O lote para de ser enviado no ponto da
  falha.

## Fila sem descarte (`overflowPolicy: block`)

Por padrão (`overflowPolicy: drop-oldest`) a fila descarta o registro mais antigo ao atingir
`maxDepth`. Para espelhos que **não podem perder nada**:

```yaml
sinks:
  - id: dev-cdc-mirror
    bootstrapServers: "kafka-dev:9092"
    acks: all
    queue:
      maxDepth: 200000
      overflowPolicy: block
      onWriteError: halt
      onPoison: halt
      retentionTime: 0s
      durability: sync-on-commit
```

- Com `block` a fila **nunca descarta** por contagem. `maxDepth` vira uma **marca d'água**: ao
  atingi-la o destino é considerado saturado e a origem é freada.
- **Backpressure sem rebalance:** o `SourceConsumer` faz `pause()` das partições cujos tópicos
  roteiam para um destino saturado e **continua chamando `poll`** (que não devolve registros
  das partições pausadas). O consumer group mantém heartbeat e `max.poll.interval.ms`, então
  não sofre rebalance; o que não foi lido continua no tópico de origem. As partições voltam
  (`resume()`) quando a fila drena até **metade** de `maxDepth` (histerese). A fila pode
  exceder `maxDepth` em até um lote (`maxPollRecords`) por origem.
- Um destino saturado freia todo tópico que roteia para ele, inclusive o fan-out para
  destinos saudáveis. Por isso use um **sink dedicado** para o espelho sem perda e não o
  compartilhe com rotas que aceitam descarte.
- **`block` exige, na validação da subida:** `retentionTime: 0s` (a expiração por tempo
  descartaria registros), `onWriteError: halt` (com `drop` uma falha de escrita local
  descartaria o registro) e `onPoison: halt` (veja abaixo). Qualquer outro valor é erro de
  configuração e a aplicação não sobe.
- **Registro rejeitado em definitivo (`onPoison`).** Um erro permanente do Kafka
  (`RecordTooLargeException`, `TopicAuthorizationException`, `ClusterAuthorizationException`,
  `OutOfOrderSequenceException`, `UnknownProducerIdException`, `InvalidTopicException`, ...) ou um
  payload acima de `maxRequestSize` não pode ser reentregue. Com `onPoison: drop` (padrão) o
  registro é descartado e contado em `poisoned_total` — como o offset da origem já foi
  commitado, ele é **perdido**. Com `onPoison: halt` o forwarder **daquele sink** para de
  entregar e **não confirma** o registro, que fica na cabeça da fila; loga `ERROR` com
  tópico/partição/offset de origem e o motivo e expõe `sink.<id>.halted` (1/0) e a linha
  `halted=YES (...)` no relatório de stats. A fila segue crescendo até saturar; com `block` a
  origem pausa. As demais rotas e sinks seguem normais e a aplicação não cai. **Retomar exige
  corrigir o destino (limite do tópico, ACL, ...) e reiniciar o processo**, que reenvia o
  registro; se ainda for inválido, para de novo.
- Não coberto pela garantia de zero perda: apagamento do disco da fila e **headers com chave
  repetida** (o relay guarda um valor por chave de header; o último vence). Alerte em
  `poisoned_total`, `offer_errors_total` e `halted`.
- Mantenha o `max.message.bytes` do tópico de destino **maior que o `batch.size` do producer**
  (16 KB por padrão). Com o limite abaixo disso o cliente Kafka 3.9 pode dividir em laço o batch
  rejeitado e estourar a pilha; o relay não perde nem descarta nada nesse caso (o lote fica na
  fila), mas também não classifica o erro nem para: ajuste o limite ou o `batch.size`.
- Garantia: at-least-once. O espelho pode duplicar (crash, retry parcial), mas não perde pelos
  mecanismos acima. Em
  retry de um lote parcialmente entregue, registros além do ponto de falha podem ter sido
  entregues antes e são reenviados em ordem; o consumidor do destino deve ser idempotente.

Exemplo completo: [`docs/examples/cdc-mirror-preserve.yaml`](docs/examples/cdc-mirror-preserve.yaml).

## Notas da versão 2.4.0 e upgrade

- Novos: `partitioning: preserve`, `overflowPolicy: block`, `onPoison: halt`, checagem de
  partições na subida, backpressure por `pause()`/`resume()` (inclusive logo após um rebalance).
  Os padrões mantêm o comportamento anterior.
- **Efeito em sinks com `acks` diferente de `all`** (inclui o padrão `acks: "1"`): o producer passa a
  usar `max.in.flight.requests.per.connection=1`, para que o retry de um batch não reordene
  registros dentro da partição. Como o forwarder já envia o lote, faz `flush` e espera a confirmação
  antes de seguir, o impacto esperado é pequeno, mas **não foi medido**; quem precisar de mais paralelismo
  usa `acks: all` (idempotência, ordem garantida) ou define a propriedade em `extraProps`.
- **Headers:** a ordem dos headers é preservada e um header com valor nulo deixou de causar
  `NullPointerException`. Headers com a mesma chave repetida ainda colapsam num só (limitação conhecida).
- **Fila em disco:** a partição de destino é um campo novo do registro persistido; filas gravadas
  pela 2.3.x continuam legíveis. **Antes de voltar para a 2.3.x, drene as filas** (e pare a
  origem): a 2.3.x ignora `destinationPartition` e **uma fila com registro de header de valor nulo
  gravada pela 2.4.0 provoca `NullPointerException` na 2.3.1**.
- Use `queue.basePath` **absoluto**, em volume persistente. Com caminho relativo a fila depende do
  diretório de trabalho do processo e um reinício em outro diretório começa com a fila vazia,
  deixando para trás os registros ainda não entregues.

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
> mvn -f /path/to/nishi-utils-spring/pom.xml clean install
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
- **Bounded drop-oldest (default):** cada fila é limitada por `maxDepth` (contagem) e/ou `retentionTime` (tempo); ao estourar, os registros **mais antigos não entregues são descartados** (perda intencional, contabilizada em `dropped`/`expired`).
- **Sem descarte por fila (`overflowPolicy: block`):** a fila não descarta por contagem nem tempo; a origem é freada por `pause()`/`resume()` enquanto o destino estiver saturado. Exige `retentionTime: 0s`, `onWriteError: halt` e `onPoison: halt` (registro rejeitado em definitivo para o destino em vez de ser descartado).

## Documentação
- [Arquitetura](docs/architecture.md) — componentes, garantias de corretude, métricas.
- [Exemplo de uso (AcmePay)](docs/usage-example.md) — cenário real de espelhamento entre datacenters, com [config pronta](docs/examples/acmepay-relay.yaml).
