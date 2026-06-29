package dev.nishisan.nstreamreplay;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Ponto de entrada do n-stream-replay.
 *
 * <p>Relay Kafka store-and-forward: cada {@code origem} é consumida e seus registros são
 * enfileirados, sem transformação, em uma fila durável por {@code destino}; um pipeline
 * amarra uma origem a N destinos (fan-out 1->N). Se um destino está offline, suas mensagens
 * permanecem na fila e são re-entregues quando ele volta, sem travar a origem nem os demais
 * destinos.</p>
 *
 * <p>A aplicação é um app Spring Boot web servlet apenas para expor as métricas via
 * {@code /actuator/prometheus} (ponte StatsUtils -> Micrometer do nishi-utils-spring).
 * O I/O Kafka usa {@code kafka-clients} cru, fora do ciclo de request do Spring.</p>
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class NStreamReplayApplication {

    public static void main(String[] args) {
        SpringApplication.run(NStreamReplayApplication.class, args);
    }
}
