package dev.nishisan.nstreamreplay.sink;

import dev.nishisan.nstreamreplay.config.Durability;
import dev.nishisan.nstreamreplay.config.OnWriteError;
import dev.nishisan.nstreamreplay.config.QueueProperties;
import dev.nishisan.nstreamreplay.model.ReplayRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class SinkChannelTest {

    private static ReplayRecord rec(int n) {
        return new ReplayRecord("t", 0, n, n, null,
                String.valueOf(n).getBytes(StandardCharsets.UTF_8), Map.of());
    }

    private static SinkChannel channel(Path tmp, FakeSink sink, Durability durability) throws Exception {
        QueueProperties cfg = new QueueProperties(tmp.toString(), 1_000_000, Duration.ZERO,
                durability, OnWriteError.DROP, 64, 0L);
        DurableSinkQueue queue = DurableSinkQueue.open("d1", cfg);
        SinkForwarder forwarder = new SinkForwarder("d1", queue, sink, 0L);
        return new SinkChannel("d1", queue, sink, forwarder, durability);
    }

    @Test
    void threadDoForwarderDrenaOEnfileiradoEEncerraLimpo(@TempDir Path tmp) throws Exception {
        FakeSink sink = new FakeSink();
        try (SinkChannel ch = channel(tmp, sink, Durability.OS_MANAGED)) {
            ch.startForwarder();
            for (int i = 0; i < 500; i++) {
                ch.offer(rec(i));
            }
            await().atMost(Duration.ofSeconds(5)).until(() -> ch.depth() == 0);

            assertThat(sink.delivered).hasSize(500);
            assertThat(ch.publishedTotal()).isEqualTo(500);
            assertThat(ch.online()).isTrue();
            assertThat(ch.dropped()).isZero();
        }
    }

    @Test
    void requiresSyncBeforeCommitSomenteParaSyncOnCommit(@TempDir Path tmp) throws Exception {
        try (SinkChannel onCommit = channel(tmp.resolve("a"), new FakeSink(), Durability.SYNC_ON_COMMIT)) {
            assertThat(onCommit.requiresSyncBeforeCommit()).isTrue();
        }
        try (SinkChannel osManaged = channel(tmp.resolve("b"), new FakeSink(), Durability.OS_MANAGED)) {
            assertThat(osManaged.requiresSyncBeforeCommit()).isFalse();
        }
        try (SinkChannel perRecord = channel(tmp.resolve("c"), new FakeSink(), Durability.PER_RECORD_FSYNC)) {
            assertThat(perRecord.requiresSyncBeforeCommit()).isFalse();
        }
    }
}
