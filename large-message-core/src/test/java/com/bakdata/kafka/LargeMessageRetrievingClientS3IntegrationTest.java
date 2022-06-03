/*
 * MIT License
 *
 * Copyright (c) 2022 bakdata
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

import static com.bakdata.kafka.AmazonS3ClientTest.deleteBucket;
import static com.bakdata.kafka.LargeMessageRetrievingClientTest.serializeUri;
import static com.bakdata.kafka.LargeMessageStoringClientS3IntegrationTest.configureS3HTTPService;
import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.collect.ImmutableMap;
import io.confluent.common.config.ConfigDef;
import java.io.ByteArrayInputStream;
import java.util.Map;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

class LargeMessageRetrievingClientS3IntegrationTest {

    @RegisterExtension
    static final S3MockExtension S3_MOCK = S3MockExtension.builder().silent()
            .withSecureConnection(false).build();
    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();

    @BeforeAll
    static void setUp() {
        configureS3HTTPService();
    }

    private static Map<String, Object> createProperties() {
        return ImmutableMap.<String, Object>builder()
                .put(AbstractLargeMessageConfig.S3_ENDPOINT_CONFIG, "http://localhost:" + S3_MOCK.getHttpPort())
                .put(AbstractLargeMessageConfig.S3_REGION_CONFIG, "us-east-1")
                .put(AbstractLargeMessageConfig.S3_ACCESS_KEY_CONFIG, "foo")
                .put(AbstractLargeMessageConfig.S3_SECRET_KEY_CONFIG, "bar")
                .build();
    }

    private static void store(final String bucket, final String key, final String s) {
        S3_MOCK.createS3Client().putObject(bucket, key, new ByteArrayInputStream(s.getBytes()), new ObjectMetadata());
    }

    private static byte[] createBackedText(final String bucket, final String key) {
        final String uri = "s3://" + bucket + "/" + key;
        return serializeUri(uri);
    }

    private static LargeMessageRetrievingClient createRetriever() {
        final Map<String, Object> properties = createProperties();
        final ConfigDef configDef = AbstractLargeMessageConfig.baseConfigDef();
        final AbstractLargeMessageConfig config = new AbstractLargeMessageConfig(configDef, properties);
        return config.getRetriever();
    }

    @Test
    void shouldReadBackedText() {
        final String bucket = "bucket";
        final S3Client s3 = S3_MOCK.createS3ClientV2();
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        final String key = "key";
        store(bucket, key, "foo");
        final LargeMessageRetrievingClient retriever = createRetriever();
        assertThat(retriever.retrieveBytes(createBackedText(bucket, key), new RecordHeaders(), false))
                .isEqualTo(STRING_SERIALIZER.serialize(null, "foo"));
        deleteBucket(bucket, s3);
    }

}
