package dev.nishisan.nstreamreplay.sink;

import dev.nishisan.nstreamreplay.model.ReplayRecord;

import java.io.IOException;

/**
 * Alvo de enfileiramento de um destino, visto pela origem. Desacopla o {@code SourceConsumer}
 * dos detalhes do {@link SinkChannel} (fila/sink/forwarder), facilitando o teste do fan-out e
 * do contrato de commit.
 */
public interface SinkTarget {

    String id();

    /**
     * Enfileira o registro na fila durável deste destino (não bloqueia).
     *
     * @return {@code true} se admitido; {@code false} se descartado (fila cheia ou política
     *         {@code onWriteError=drop} em falha de escrita local)
     * @throws IOException somente sob {@code onWriteError=halt} em falha de escrita local
     */
    boolean offer(ReplayRecord record) throws IOException;

    /** A durabilidade exige {@code sync()} antes do commit da origem? (apenas SYNC_ON_COMMIT). */
    boolean requiresSyncBeforeCommit();

    /** Força fsync da fila (group-commit antes do commit da origem). */
    void sync() throws IOException;
}
