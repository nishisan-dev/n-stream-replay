package dev.nishisan.nstreamreplay.decoder;

import dev.nishisan.nstreamreplay.route.PartitionKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.*;

class DecoderTest {
    private byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    @ParameterizedTest
    @ValueSource(strings = {"json", "yaml"})
    void extractsScalarPropertiesWithoutChangingContent(String format) throws Exception {
        var document = Decoders.create(format).decode(bytes("""
                {"payload":{"Id":" áBC ","nested":{"n":123456789012345678901234567890}},
                 "empty":"","yes":true,"zero":0,"number":1.25,"id":"root"}
                """));
        assertThat(new PartitionKey("payload.Id").resolve(document).key()).isEqualTo(bytes(" áBC "));
        assertThat(new PartitionKey("payload.nested.n").resolve(document).key())
                .isEqualTo(bytes("123456789012345678901234567890"));
        assertThat(new PartitionKey("empty").resolve(document).key()).isEmpty();
        assertThat(new PartitionKey("yes").resolve(document).key()).isEqualTo(bytes("true"));
        assertThat(new PartitionKey("zero").resolve(document).key()).isEqualTo(bytes("0"));
        assertThat(new PartitionKey("number").resolve(document).key()).isEqualTo(bytes("1.25"));
        assertThat(new PartitionKey("id").resolve(document).key()).isEqualTo(bytes("root"));
        assertThat(new PartitionKey("payload.id").resolve(document).failure()).isEqualTo(PartitionKey.Failure.MISSING);
    }

    @Test
    void readsYamlMappingAndText() throws Exception {
        var yaml = Decoders.create("yaml").decode(bytes("payload:\n  Id: abc\n"));
        assertThat(new PartitionKey("payload.Id").resolve(yaml).key()).isEqualTo(bytes("abc"));
        assertThat(Decoders.create("text").decode(bytes("hello\n")).textValue()).isEqualTo("hello\n");
        assertThat(Decoders.create("text").decode(null).isNull()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"json", "yaml"})
    void rejectsMalformedEmptyAndMultipleDocuments(String format) {
        Decoder decoder = Decoders.create(format);
        for (String input : new String[]{"{", "", "   ", "{\"id\":1} {\"id\":2}", "{\"id\":1,\"id\":2}"}) {
            assertThatThrownBy(() -> decoder.decode(bytes(input))).as(format + ": " + input).isInstanceOf(IOException.class);
        }
        if (format.equals("yaml")) {
            assertThatThrownBy(() -> decoder.decode(bytes("id: a\n---\nid: b"))).isInstanceOf(IOException.class);
        }
    }

    @Test
    void missingNullAndInvalidTypesHaveDistinctOutcomes() throws Exception {
        var decoder = Decoders.create("json");
        for (String input : new String[]{"{}", "{\"id\":null}", "null"}) {
            assertThat(new PartitionKey("id").resolve(decoder.decode(bytes(input))).failure())
                    .isEqualTo(PartitionKey.Failure.MISSING);
        }
        assertThat(new PartitionKey("id").resolve(decoder.decode(null)).failure()).isEqualTo(PartitionKey.Failure.MISSING);
        for (String input : new String[]{"{\"id\":{}}", "{\"id\":[]}", "[]", "123", "true"}) {
            assertThat(new PartitionKey("id").resolve(decoder.decode(bytes(input))).failure())
                    .isEqualTo(PartitionKey.Failure.INVALID_TYPE);
        }
        var array = decoder.decode(bytes("{\"items\":[{\"id\":1}]}"));
        assertThat(new PartitionKey("items.0.id").resolve(array).failure()).isEqualTo(PartitionKey.Failure.INVALID_TYPE);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", ".id", "id.", "a..b", "a[0]", "a.*", "a b", "$.id()", "a/b"})
    void rejectsInvalidPaths(String path) {
        assertThatThrownBy(() -> new PartitionKey(path)).isInstanceOf(IllegalStateException.class);
    }
}
