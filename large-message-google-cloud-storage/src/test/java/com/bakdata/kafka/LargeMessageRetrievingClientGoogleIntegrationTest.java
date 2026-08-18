/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

class LargeMessageRetrievingClientGoogleIntegrationTest extends GoogleCloudStorageIntegrationTest {

    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();

    private static void store(final Storage storage, final String bucket, final String key, final String s) {
        final BlobId blobId = BlobId.of(bucket, key);
        final BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        storage.create(blobInfo, s.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] createBackedText(final String bucket, final String key) {
        final String uri = "gs://" + bucket + "/" + key;
        return TestHelper.serializeUri(uri);
    }

    @Test
    void shouldReadBackedText() throws Exception {
        final String bucket = "bucket";
        try (final Storage storage = this.getStorage()) {
            storage.create(BucketInfo.newBuilder(bucket).build());
            final String key = "key";
            store(storage, bucket, key, "foo");
            try (final LargeMessageRetrievingClient retriever = this.createRetriever()) {
                assertThat(retriever.retrieveBytes(createBackedText(bucket, key), new RecordHeaders(), false))
                        .isEqualTo(STRING_SERIALIZER.serialize(null, "foo"));
            }
        }
    }

    private Map<String, Object> createProperties() {
        return Map.of(
                GoogleCloudStorageConfig.GOOGLE_CLOUD_URL_CONFIG, this.getUrl(),
                GoogleCloudStorageConfig.GOOGLE_CLOUD_PROJECT_CONFIG, PROJECT
        );
    }

    private LargeMessageRetrievingClient createRetriever() {
        final Map<String, Object> properties = this.createProperties();
        final AbstractLargeMessageConfig config = new AbstractLargeMessageConfig(properties);
        return config.getRetriever();
    }

}
