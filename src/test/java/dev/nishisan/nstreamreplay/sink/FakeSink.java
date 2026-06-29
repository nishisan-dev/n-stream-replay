package dev.nishisan.nstreamreplay.sink;

import dev.nishisan.nstreamreplay.model.ReplayRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * {@link Sink} de teste com comportamento roteirizável por chamada (entrega total, broker fora,
 * poison, parcial). Captura os itens entregues (prefixo {@code published} de cada lote).
 */
final class FakeSink implements Sink {

    volatile Function<List<ReplayRecord>, PublishOutcome> behavior = b -> new PublishOutcome(b.size(), 0);
    final List<ReplayRecord> delivered = Collections.synchronizedList(new ArrayList<>());

    static PublishOutcome allOk(List<ReplayRecord> batch) {
        return new PublishOutcome(batch.size(), 0);
    }

    static PublishOutcome brokerDown(List<ReplayRecord> batch) {
        return PublishOutcome.none();
    }

    @Override
    public PublishOutcome publish(List<ReplayRecord> batch) {
        PublishOutcome o = behavior.apply(batch);
        for (int i = 0; i < o.published(); i++) {
            delivered.add(batch.get(i));
        }
        return o;
    }
}
