package com.bakdata.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;

import org.junit.jupiter.api.Test;

class CompressionTypeTest {
    @Test
    void shouldRoundtrip() {
        for (CompressionType compressionType : CompressionType.values()) {
            byte[] original = new byte[20];
            new Random().nextBytes(original);
            byte[] compressed = compressionType.compress(original);
            byte[] decompressed = compressionType.decompress(compressed);
            assertThat(decompressed.length).isEqualTo(original.length);
            assertThat(decompressed).isEqualTo(original);
        }
    }
}
