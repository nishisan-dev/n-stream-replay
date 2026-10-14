package dev.nishisan.nstreamreplay.decoder;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

/** Read-only view of a payload; decoding never changes the bytes sent downstream. */
@FunctionalInterface
public interface Decoder {
    JsonNode decode(byte[] payload) throws IOException;
}
