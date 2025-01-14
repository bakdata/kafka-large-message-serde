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

import static com.bakdata.kafka.LargeMessageRetrievingClientTest.serializeUri;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class LargeMessageRetrievingClientS3IntegrationTest extends AmazonS3IntegrationTest {

    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();

    private static byte[] createBackedText(final String bucket, final String key) {
        final String uri = "s3://" + bucket + "/" + key;
        return serializeUri(uri);
    }

    @Test
    void shouldReadBackedText() {
        final String bucket = "bucket";
        final S3Client s3 = this.getS3Client();
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        final String key = "key";
        this.store(bucket, key, "foo");
        try (final LargeMessageRetrievingClient retriever = this.createRetriever()) {
            assertThat(retriever.retrieveBytes(createBackedText(bucket, key), new RecordHeaders(), false))
                    .isEqualTo(STRING_SERIALIZER.serialize(null, "foo"));
        }
    }

    private LargeMessageRetrievingClient createRetriever() {
        final Map<String, String> properties = this.getLargeMessageConfig();
        final AbstractLargeMessageConfig config = new AbstractLargeMessageConfig(properties);
        return config.getRetriever();
    }

    private void store(final String bucket, final String key, final String s) {
        this.getS3Client().putObject(PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build(), RequestBody.fromString(s));
    }

}
