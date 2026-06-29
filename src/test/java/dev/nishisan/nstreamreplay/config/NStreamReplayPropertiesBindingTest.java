package dev.nishisan.nstreamreplay.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ByteArrayResource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifica o binding real ({@link Binder}) de YAML para {@link NStreamReplayProperties},
 * exercitando {@code @DefaultValue}, binding relaxado de enum (kebab-case) e {@link Duration}.
 */
class NStreamReplayPropertiesBindingTest {

    private static NStreamReplayProperties bind(String yaml) {
        StandardEnvironment env = new StandardEnvironment();
        List<PropertySource<?>> sources;
        try {
            sources = new YamlPropertySourceLoader()
                    .load("test", new ByteArrayResource(yaml.getBytes(StandardCharsets.UTF_8)));
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        sources.forEach(env.getPropertySources()::addLast);
        return Binder.get(env).bind("nstreamreplay", NStreamReplayProperties.class).get();
    }

    @Test
    void ligaTodosOsCamposDeUmSinkCompleto() {
        String yaml = """
                nstreamreplay:
                  sources:
                    - id: s1
                      bootstrapServers: "host-a:9092"
                      topics: [ "orders.in", "aux.in" ]
                      groupId: "g1"
                      clientId: "cli-1"
                      autoOffsetReset: earliest
                      maxPollRecords: 250
                      pollTimeoutMs: 750
                      extraProps:
                        security.protocol: SASL_SSL
                  sinks:
                    - id: d1
                      bootstrapServers: "host-b:9092"
                      topic: "orders.mirror"
                      acks: all
                      lingerMs: 33
                      compressionType: zstd
                      maxRequestSize: 2097152
                      queue:
                        basePath: "var/q"
                        maxDepth: 12345
                        retentionTime: 6h
                        durability: per-record-fsync
                        onWriteError: halt
                        batchSize: 64
                        forwarderRetryBackoffMs: 2500
                  pipelines:
                    - id: p1
                      source: s1
                      sinks: [ d1 ]
                """;

        NStreamReplayProperties props = bind(yaml);

        SourceProperties s = props.sources().get(0);
        assertThat(s.id()).isEqualTo("s1");
        assertThat(s.topics()).containsExactly("orders.in", "aux.in");
        assertThat(s.autoOffsetReset()).isEqualTo("earliest");
        assertThat(s.maxPollRecords()).isEqualTo(250);
        assertThat(s.pollTimeoutMs()).isEqualTo(750L);
        assertThat(s.resolvedClientId()).isEqualTo("cli-1");
        assertThat(s.extraProps()).containsEntry("security.protocol", "SASL_SSL");

        SinkProperties d = props.sinks().get(0);
        assertThat(d.topic()).isEqualTo("orders.mirror");
        assertThat(d.acks()).isEqualTo("all");
        assertThat(d.compressionType()).isEqualTo("zstd");
        assertThat(d.maxRequestSize()).isEqualTo(2_097_152);

        QueueProperties q = d.queue();
        assertThat(q.maxDepth()).isEqualTo(12345);
        assertThat(q.retentionTime()).isEqualTo(Duration.ofHours(6));
        assertThat(q.hasRetentionTime()).isTrue();
        assertThat(q.durability()).isEqualTo(Durability.PER_RECORD_FSYNC);
        assertThat(q.onWriteError()).isEqualTo(OnWriteError.HALT);
        assertThat(q.batchSize()).isEqualTo(64);
        assertThat(q.forwarderRetryBackoffMs()).isEqualTo(2500L);

        assertThat(props.pipelines().get(0).sinks()).containsExactly("d1");
    }

    @Test
    void aplicaDefaultsQuandoCamposEBlocoQueueAusentes() {
        String yaml = """
                nstreamreplay:
                  sources:
                    - id: s1
                      bootstrapServers: "host:9092"
                      topics: [ "t" ]
                      groupId: "g"
                  sinks:
                    - id: d1
                      bootstrapServers: "host:9092"
                      topic: "t.out"
                  pipelines:
                    - id: p1
                      source: s1
                      sinks: [ d1 ]
                """;

        NStreamReplayProperties props = bind(yaml);

        SourceProperties s = props.sources().get(0);
        assertThat(s.autoOffsetReset()).isEqualTo("latest");
        assertThat(s.maxPollRecords()).isEqualTo(500);
        assertThat(s.pollTimeoutMs()).isEqualTo(1000L);
        assertThat(s.resolvedClientId()).isEqualTo("s1");
        assertThat(s.extraProps()).isEmpty();

        SinkProperties d = props.sinks().get(0);
        assertThat(d.acks()).isEqualTo("1");
        assertThat(d.lingerMs()).isEqualTo(50);
        assertThat(d.compressionType()).isEqualTo("lz4");
        assertThat(d.maxRequestSize()).isEqualTo(1_048_576);
        assertThat(d.extraProps()).isEmpty();

        // Bloco queue inteiramente ausente -> instância com todos os defaults.
        QueueProperties q = d.queue();
        assertThat(q).isNotNull();
        assertThat(q.basePath()).isEqualTo("var/n-stream-replay/queues");
        assertThat(q.maxDepth()).isEqualTo(100_000);
        assertThat(q.retentionTime()).isEqualTo(Duration.ZERO);
        assertThat(q.hasRetentionTime()).isFalse();
        assertThat(q.durability()).isEqualTo(Durability.SYNC_ON_COMMIT);
        assertThat(q.onWriteError()).isEqualTo(OnWriteError.DROP);
        assertThat(q.batchSize()).isEqualTo(256);
        assertThat(q.forwarderRetryBackoffMs()).isEqualTo(1000L);
    }

    @Test
    void ligaRotasComToTopicOpcional() {
        String yaml = """
                nstreamreplay:
                  sources:
                    - { id: s1, bootstrapServers: "h:9092", topics: [ t ], groupId: g }
                  sinks:
                    - { id: d1, bootstrapServers: "h:9092", topic: out }
                  pipelines:
                    - { id: p1, source: s1, sinks: [ d1 ] }
                  routes:
                    - id: r1
                      source: s1
                      fromTopic: orders.in
                      to:
                        - { sink: d1 }
                        - { sink: d1, toTopic: orders.raw }
                """;

        NStreamReplayProperties props = bind(yaml);

        RouteProperties r = props.routes().get(0);
        assertThat(r.id()).isEqualTo("r1");
        assertThat(r.source()).isEqualTo("s1");
        assertThat(r.fromTopic()).isEqualTo("orders.in");
        assertThat(r.to()).hasSize(2);
        assertThat(r.to().get(0).sink()).isEqualTo("d1");
        assertThat(r.to().get(0).toTopic()).isNull();              // preserva
        assertThat(r.to().get(1).toTopic()).isEqualTo("orders.raw");
    }
}
