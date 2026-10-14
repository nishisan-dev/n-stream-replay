package dev.nishisan.nstreamreplay.route;

import com.fasterxml.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

/** Compiled property path, deliberately not an expression or array-index language. */
public final class PartitionKey {
    private static final Pattern PATH = Pattern.compile("[\\p{L}\\p{N}_$-]+(?:\\.[\\p{L}\\p{N}_$-]+)*");
    private final String[] segments;

    public PartitionKey(String path) {
        if (path == null || !PATH.matcher(path).matches()) {
            throw new IllegalStateException("partitionKey inválido: '" + path + "' (use propriedades separadas por ponto)");
        }
        segments = path.split("\\.");
    }

    public Result resolve(JsonNode payload) {
        JsonNode node = payload;
        for (String segment : segments) {
            if (node == null || node.isNull() || node.isMissingNode()) {
                return new Result(null, Failure.MISSING);
            }
            if (!node.isObject()) {
                return new Result(null, Failure.INVALID_TYPE);
            }
            node = node.get(segment);
        }
        if (node == null || node.isNull()) {
            return new Result(null, Failure.MISSING);
        }
        if (!node.isTextual() && !node.isNumber() && !node.isBoolean()) {
            return new Result(null, Failure.INVALID_TYPE);
        }
        return new Result(node.asText().getBytes(StandardCharsets.UTF_8), null);
    }

    public enum Failure { MISSING, INVALID_TYPE }

    public record Result(byte[] key, Failure failure) { }
}
