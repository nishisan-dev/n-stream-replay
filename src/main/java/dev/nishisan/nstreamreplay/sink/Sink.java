package dev.nishisan.nstreamreplay.sink;

import dev.nishisan.nstreamreplay.model.ReplayRecord;

import java.util.List;

/**
 * Destino de transporte do forwarder — abstrai o broker para testar a lógica de
 * store-and-forward sem um Kafka real. Semântica FIFO: publica o lote na ordem e devolve, via
 * {@link PublishOutcome}, o prefixo de cabeça contíguo que o forwarder deve confirmar (itens
 * entregues + itens descartados em definitivo), parando no primeiro erro transitório.
 * Implementação de produção: {@link KafkaSink}.
 */
public interface Sink extends AutoCloseable {

    /**
     * Publica o lote, em ordem, e devolve o {@link PublishOutcome}. {@link PublishOutcome#none()}
     * = broker indisponível no primeiro item (tudo permanece na fila). Pode lançar — o forwarder
     * trata como {@code none()} e faz backoff.
     */
    PublishOutcome publish(List<ReplayRecord> batch);

    @Override
    default void close() {
    }
}
