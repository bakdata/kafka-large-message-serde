package com.bakdata.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class CompressionTypeTest {
    @ParameterizedTest
    @MethodSource("provideParameters")
    void shouldRoundtrip(CompressionType compressionType) {
        byte[] original = new byte[20];
        new Random().nextBytes(original);
        byte[] compressed = compressionType.compress(original);
        byte[] decompressed = compressionType.decompress(compressed);
        assertThat(decompressed).isEqualTo(original);
    }

    private static CompressionType[] provideParameters() {
        return CompressionType.values();
    }
}
