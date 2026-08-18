package com.bakdata.kafka;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class AbstractLargeMessageConfigTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldStoreAndRetrieve() {
        final AbstractLargeMessageConfig config = new AbstractLargeMessageConfig(Map.of(
                AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, 0,
                AbstractLargeMessageConfig.BASE_PATH_CONFIG, TestBlobStorageConfig.SCHEME + "://bucket"
        ));
        try (final LargeMessageStoringClient storer = config.getStorer();
                final LargeMessageRetrievingClient retriever = config.getRetriever()) {
            final byte[] data = "foo".getBytes(StandardCharsets.UTF_8);
            final boolean isKey = false;
            final byte[] bytes = storer.storeBytes("topic", data, isKey);
            final byte[] retrieved = retriever.retrieveBytes(bytes, isKey);
            this.softly.assertThat(retrieved).isEqualTo(data);
        }
    }

    @Test
    void shouldNotLoadInvalidConfig() {
        final AbstractLargeMessageConfig config = new AbstractLargeMessageConfig(Map.of(
                AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, 0,
                AbstractLargeMessageConfig.BASE_PATH_CONFIG, InvalidBlobStorageConfig.SCHEME + "://bucket"
        ));
        this.softly.assertThatThrownBy(config::getStorer)
                .isInstanceOf(SerializationException.class)
                .hasMessage("Unknown scheme for handling large messages: '%s'", InvalidBlobStorageConfig.SCHEME);
    }

}
