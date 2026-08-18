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

import static com.bakdata.kafka.TestHelper.deserializeUriWithFlag;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

class LargeMessageStoringClientGoogleIntegrationTest extends GoogleCloudStorageIntegrationTest {

    private static final String TOPIC = "output";
    private static final Deserializer<String> STRING_DESERIALIZER = Serdes.String().deserializer();
    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();

    private static byte[] serialize(final String s) {
        return STRING_SERIALIZER.serialize(null, s);
    }

    @Test
    void shouldWriteBackedTextKey() throws Exception {
        final String bucket = "bucket";
        final String basePath = "gs://" + bucket + "/base/";
        final Map<String, Object> properties = Map.of(
                AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, 0,
                AbstractLargeMessageConfig.BASE_PATH_CONFIG, basePath
        );
        try (final Storage storage = this.getStorage()) {
            storage.create(BucketInfo.newBuilder(bucket).build());
            try (final LargeMessageStoringClient storer = this.createStorer(properties)) {
                assertThat(storer.storeBytes(TOPIC, serialize("foo"), true, new RecordHeaders()))
                        .satisfies(backedText -> this.expectBackedText(basePath, "foo", backedText, "keys"));
            }
        }
    }

    private Map<String, Object> createProperties(final Map<String, Object> properties) {
        return ImmutableMap.<String, Object>builder()
                .putAll(properties)
                .put(GoogleCloudStorageConfig.GOOGLE_CLOUD_URL_CONFIG, this.getUrl())
                .put(GoogleCloudStorageConfig.GOOGLE_CLOUD_PROJECT_CONFIG, PROJECT)
                .build();
    }

    private void expectBackedText(final String basePath, final String expected, final byte[] backedText,
            final String type) {
        final BlobStorageURI uri = deserializeUriWithFlag(backedText);
        this.expectBackedText(basePath, expected, uri, type);
    }

    private void expectBackedText(final String basePath, final String expected, final BlobStorageURI uri,
            final String type) {
        assertThat(uri).asString().startsWith(basePath + TOPIC + "/" + type + "/");
        final byte[] bytes = this.readBytes(uri);
        final String deserialized = STRING_DESERIALIZER.deserialize(null, bytes);
        assertThat(deserialized).isEqualTo(expected);
    }

    private byte[] readBytes(final BlobStorageURI uri) {
        try (final Storage storage = this.getStorage()) {
            return storage.get(BlobId.of(uri.getBucket(), uri.getKey())).getContent();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private LargeMessageStoringClient createStorer(final Map<String, Object> baseProperties) {
        final Map<String, Object> properties = this.createProperties(baseProperties);
        final AbstractLargeMessageConfig config = new AbstractLargeMessageConfig(properties);
        return config.getStorer();
    }
}
