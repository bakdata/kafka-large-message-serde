package com.bakdata.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class CompressionTypeTest {
    @ParameterizedTest
    @EnumSource
    void shouldRoundtrip(CompressionType compressionType) {
        byte[] original = new byte[20];
        new Random().nextBytes(original);
        byte[] compressed = compressionType.compress(original);
        byte[] decompressed = compressionType.decompress(compressed);
        assertThat(decompressed).isEqualTo(original);
    }
}
