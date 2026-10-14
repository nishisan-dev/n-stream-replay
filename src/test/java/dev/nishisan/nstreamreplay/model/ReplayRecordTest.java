package dev.nishisan.nstreamreplay.model;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ReplayRecordTest {

    private static ReplayRecord roundTrip(ReplayRecord original) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(original);
        }
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            return (ReplayRecord) ois.readObject();
        }
    }

    @Test
    void sobreviveAoRoundTripDeSerializacaoJava() throws Exception {
        Map<String, byte[]> headers = new LinkedHashMap<>();
        headers.put("trace-id", "abc".getBytes(StandardCharsets.UTF_8));
        ReplayRecord original = new ReplayRecord(
                "orders.in", 3, 42L, 1_700_000_000_000L,
                "k1".getBytes(StandardCharsets.UTF_8),
                "payload".getBytes(StandardCharsets.UTF_8),
                headers, "orders.mirror");

        ReplayRecord restored = roundTrip(original);

        assertThat(restored).isEqualTo(original);
        assertThat(restored.sourceTopic()).isEqualTo("orders.in");
        assertThat(restored.destinationTopic()).isEqualTo("orders.mirror");
        assertThat(restored.partition()).isEqualTo(3);
        assertThat(restored.offset()).isEqualTo(42L);
        assertThat(restored.value()).asString(StandardCharsets.UTF_8).isEqualTo("payload");
        assertThat(restored.headers().get("trace-id")).asString(StandardCharsets.UTF_8).isEqualTo("abc");
    }

    @Test
    void suportaKeyEValueNulosTombstone() throws Exception {
        ReplayRecord tombstone = new ReplayRecord(
                "t", 0, 1L, 0L, "k".getBytes(StandardCharsets.UTF_8), null, null, "t.out");

        ReplayRecord restored = roundTrip(tombstone);

        assertThat(restored.value()).isNull();
        assertThat(restored.valueSize()).isZero();
        assertThat(restored.headers()).isEmpty();
        assertThat(restored).isEqualTo(tombstone);
    }

    @Test
    void igualdadePorConteudoIgnoraIdentidadeDeArrays() {
        ReplayRecord a = new ReplayRecord("t", 0, 1L, 9L,
                new byte[]{1, 2}, new byte[]{3, 4}, Map.of(), "t.out");
        ReplayRecord b = new ReplayRecord("t", 0, 1L, 9L,
                new byte[]{1, 2}, new byte[]{3, 4}, Map.of(), "t.out");

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void readsRecordSerializedByMainBeforeDecoders() throws Exception {
        // Fixture produced by ReplayRecord from main commit 7d07a14 using ObjectOutputStream,
        // the serialization format used by NQueue. No schema migration should be required.
        try (var input = new java.io.ObjectInputStream(getClass().getResourceAsStream("/fixtures/replay-record-v2.ser"))) {
            ReplayRecord record = (ReplayRecord) input.readObject();
            assertThat(record.sourceTopic()).isEqualTo("legacy.in");
            assertThat(record.destinationTopic()).isEqualTo("legacy.out");
            assertThat(record.offset()).isEqualTo(42L);
            assertThat(record.timestamp()).isEqualTo(1234L);
            assertThat(record.key()).isEqualTo("old-key".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            assertThat(record.value()).isEqualTo("old-value".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            assertThat(record.headers().get("header")).containsExactly((byte) 1, (byte) 2);
        }
    }
}
