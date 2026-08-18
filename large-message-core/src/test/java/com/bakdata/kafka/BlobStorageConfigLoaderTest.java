package com.bakdata.kafka;

import java.util.Collection;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class BlobStorageConfigLoaderTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldLoad() {
        final Collection<BlobStorageConfigFactory> factories = BlobStorageConfigLoader.loadConfigFactories();
        this.softly.assertThat(factories)
                .extracting(BlobStorageConfigFactory::getScheme)
                .containsExactlyInAnyOrder(TestBlobStorageConfig.SCHEME, InvalidBlobStorageConfig.SCHEME);
    }

}
