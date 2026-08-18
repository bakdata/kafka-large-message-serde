package com.bakdata.kafka;

import java.util.Collection;
import java.util.stream.Stream;
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

    @Test
    void shouldFailOnDuplicateScheme() {
        final Stream<Class<? extends BlobStorageConfig>> classes = Stream.of(
                Duplicate1.class,
                Duplicate2.class
        );
        this.softly.assertThatThrownBy(() -> BlobStorageConfigLoader.load(classes))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("Duplicate schemes found: {duplicate=[")
                .hasMessageContaining(Duplicate1.class.getName())
                .hasMessageContaining(Duplicate2.class.getName())
                .hasMessageEndingWith("]}");
    }

    @BlobStorageType("duplicate")
    private static class Duplicate1 implements BlobStorageConfig {
        @Override
        public BlobStorageClient createBlobStorageClient() {
            throw new UnsupportedOperationException();
        }
    }

    @BlobStorageType("duplicate")
    private static class Duplicate2 implements BlobStorageConfig {
        @Override
        public BlobStorageClient createBlobStorageClient() {
            throw new UnsupportedOperationException();
        }
    }

}
