package dev.nishisan.nstreamreplay.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 * Registro trafegado da origem para um destino, persistido na fila durável ({@code NQueue}
 * serializa via {@code ObjectOutputStream}, por isso {@link Serializable}). O payload mantém os bytes originais
 * do Kafka; {@link #key} é a chave original ou a chave extraída pela rota antes de enfileirar. Carrega a <b>proveniência</b> da
 * origem ({@link #sourceTopic}/{@link #partition}/{@link #offset}) e o <b>tópico de destino</b>
 * resolvido pela rota ({@link #destinationTopic}) — uma fila por sink pode conter registros para
 * tópicos de destino diferentes, e o {@code KafkaSink} publica em {@link #destinationTopic}.
 *
 * <p><b>Partição de destino:</b> {@link #destinationPartition} é a partição em que o registro deve
 * ser publicado ({@code partitioning: preserve}) ou {@code null} para deixar o producer particionar
 * pela {@link #key}. Ela é persistida junto com o registro para sobreviver à fila durável.
 * <b>Compatibilidade de formato:</b> o campo foi acrescentado ao final com o mesmo
 * {@code serialVersionUID}; registros gravados por versões anteriores (sem o campo) são lidos
 * pela desserialização de records com o valor padrão {@code null} (= particionar pela key).
 *
 * @param sourceTopic      tópico de origem
 * @param partition        partição de origem
 * @param offset           offset de origem
 * @param timestamp        timestamp do record na origem (epoch ms)
 * @param key              chave (pode ser {@code null})
 * @param value            valor/payload (pode ser {@code null} — tombstone)
 * @param headers          headers do record (nunca {@code null}; cópia imutável)
 * @param destinationTopic tópico de destino resolvido pela rota
 * @param destinationPartition partição de destino ({@code null} = particionar pela key)
 */
public record ReplayRecord(
        String sourceTopic,
        int partition,
        long offset,
        long timestamp,
        byte[] key,
        byte[] value,
        Map<String, byte[]> headers,
        String destinationTopic,
        Integer destinationPartition) implements Serializable {

    @Serial
    private static final long serialVersionUID = 2L;

    public ReplayRecord {
        // LinkedHashMap (e não Map.copyOf): preserva a ordem dos headers e admite valor nulo,
        // ambos legítimos no Kafka. Map.copyOf lançaria NPE num header sem valor.
        headers = (headers == null) ? Map.of()
                : java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(headers));
        if (destinationPartition != null && destinationPartition < 0) {
            throw new IllegalArgumentException("destinationPartition deve ser >= 0: " + destinationPartition);
        }
    }

    /** Registro sem partição de destino fixa: o producer particiona pela {@link #key}. */
    public ReplayRecord(String sourceTopic, int partition, long offset, long timestamp,
                        byte[] key, byte[] value, Map<String, byte[]> headers, String destinationTopic) {
        this(sourceTopic, partition, offset, timestamp, key, value, headers, destinationTopic, null);
    }

    /** Tamanho do payload (value) em bytes; 0 para tombstone. Usado na guarda de oversize do sink. */
    public int valueSize() {
        return value == null ? 0 : value.length;
    }

    /** Igualdade por conteúdo (arrays comparados por valor), útil em testes e dedup. */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ReplayRecord(
                String topic, int part, long off, long ts,
                byte[] k, byte[] v, Map<String, byte[]> h, String destTopic, Integer destPartition))) {
            return false;
        }
        if (partition != part || offset != off || timestamp != ts) {
            return false;
        }
        if (!java.util.Objects.equals(sourceTopic, topic)
                || !java.util.Objects.equals(destinationTopic, destTopic)
                || !java.util.Objects.equals(destinationPartition, destPartition)) {
            return false;
        }
        if (!Arrays.equals(key, k) || !Arrays.equals(value, v)) {
            return false;
        }
        return headersEqual(headers, h);
    }

    @Override
    public int hashCode() {
        int result = java.util.Objects.hash(sourceTopic, partition, offset, timestamp, destinationTopic, destinationPartition);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        result = 31 * result + headers.size();
        return result;
    }

    private static boolean headersEqual(Map<String, byte[]> a, Map<String, byte[]> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (Map.Entry<String, byte[]> e : a.entrySet()) {
            if (!b.containsKey(e.getKey()) || !Arrays.equals(e.getValue(), b.get(e.getKey()))) {
                return false;
            }
        }
        return true;
    }
}
