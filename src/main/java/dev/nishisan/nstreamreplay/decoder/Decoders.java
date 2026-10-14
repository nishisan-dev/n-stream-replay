package dev.nishisan.nstreamreplay.decoder;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Built-in decoders. Custom is reserved until a registration mechanism is available. */
public final class Decoders {
    private Decoders() { }

    public static void validate(String name) {
        if ("custom".equals(name)) {
            throw new IllegalStateException("decoder 'custom' ainda não implementado");
        }
        if (!"text".equals(name) && !"json".equals(name) && !"yaml".equals(name)) {
            throw new IllegalStateException("decoder desconhecido: " + name + " (use json, yaml ou text)");
        }
    }

    public static Decoder create(String name) {
        validate(name);
        if ("text".equals(name)) {
            return payload -> payload == null ? NullNode.instance
                    : TextNode.valueOf(new String(payload, StandardCharsets.UTF_8));
        }
        ObjectMapper mapper = "yaml".equals(name) ? new ObjectMapper(new YAMLFactory()) : new ObjectMapper();
        mapper.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        mapper.enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
        mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        return payload -> {
            if (payload == null) {
                return NullNode.instance;
            }
            var result = mapper.readTree(payload);
            if (result == null || result.isMissingNode()) {
                throw new IOException("payload vazio");
            }
            return result;
        };
    }
}
