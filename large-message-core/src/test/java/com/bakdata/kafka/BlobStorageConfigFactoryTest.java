package com.bakdata.kafka;

import java.util.Map;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class BlobStorageConfigFactoryTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldNotCreateConfig() {
        final BlobStorageConfigFactory factory = new BlobStorageConfigFactory(InvalidBlobStorageConfig.class);
        this.softly.assertThat(factory.create(Map.of())).isNotPresent();
    }

}
