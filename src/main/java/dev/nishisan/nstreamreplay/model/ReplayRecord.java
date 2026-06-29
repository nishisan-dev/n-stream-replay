package dev.nishisan.nstreamreplay.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 * Registro trafegado da origem para os destinos, persistido nas filas duráveis ({@code NQueue}
 * serializa via {@code ObjectOutputStream}, por isso {@link Serializable}). Não há transformação:
 * {@link #key} e {@link #value} são os bytes originais do Kafka. Carrega a <b>proveniência</b>
 * da origem ({@link #sourceTopic}/{@link #partition}/{@link #offset}) — barato e suficiente para
 * uma deduplicação posterior (o MVP é at-least-once) e para diagnóstico.
 *
 * @param sourceTopic tópico de origem
 * @param partition   partição de origem
 * @param offset      offset de origem
 * @param timestamp   timestamp do record na origem (epoch ms)
 * @param key         chave (pode ser {@code null})
 * @param value       valor/payload (pode ser {@code null} — tombstone)
 * @param headers     headers do record (nunca {@code null}; cópia imutável)
 */
public record ReplayRecord(
        String sourceTopic,
        int partition,
        long offset,
        long timestamp,
        byte[] key,
        byte[] value,
        Map<String, byte[]> headers) implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    public ReplayRecord {
        headers = (headers == null) ? Map.of() : Map.copyOf(headers);
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
                String topic, int part, long off, long ts, byte[] k, byte[] v, Map<String, byte[]> h))) {
            return false;
        }
        if (partition != part || offset != off || timestamp != ts) {
            return false;
        }
        if (!java.util.Objects.equals(sourceTopic, topic)) {
            return false;
        }
        if (!Arrays.equals(key, k) || !Arrays.equals(value, v)) {
            return false;
        }
        return headersEqual(headers, h);
    }

    @Override
    public int hashCode() {
        int result = java.util.Objects.hash(sourceTopic, partition, offset, timestamp);
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
