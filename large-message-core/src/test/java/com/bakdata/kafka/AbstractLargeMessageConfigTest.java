/*
 * MIT License
 *
 * Copyright (c) 2026 bakdata
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

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
