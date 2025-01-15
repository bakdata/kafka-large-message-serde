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
import static software.amazon.awssdk.core.client.config.SdkClientOption.CONFIGURED_SYNC_HTTP_CLIENT_BUILDER;
import static software.amazon.awssdk.core.client.config.SdkClientOption.SYNC_HTTP_CLIENT;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.utils.AttributeMap;

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

    @Test
    void shouldUseConfiguredSdkHttpClientBuilder() {
        final String bucket = "bucket";
        final String basePath = "s3://" + bucket + "/base/";
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractLargeMessageConfig.S3_REGION_CONFIG, "us-east-1")
                .put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, 0)
                .put(AbstractLargeMessageConfig.BASE_PATH_CONFIG, basePath)
                .put(AbstractLargeMessageConfig.S3_SDK_HTTP_CLIENT_BUILDER_CONFIG, MockSdkHttpClientBuilder.class.getName())
                .build();
        AbstractLargeMessageConfig config = new AbstractLargeMessageConfig(properties);
        LargeMessageRetrievingClient retriever = config.getRetriever();
        // Get private field clientFactories
        Map<String, Supplier<BlobStorageClient>> clientFactories = getPrivateField(retriever, "clientFactories", Map.class);
        BlobStorageClient blobStorageClient = clientFactories.get("s3").get();
        // Get private field s3Client
        S3Client s3Client = getPrivateField(blobStorageClient, "s3", S3Client.class);
        // Get private field clientConfiguration
        SdkClientConfiguration clientConfiguration = getPrivateField(s3Client, "clientConfiguration", SdkClientConfiguration.class);
        // Get private field attributes
        AttributeMap attributes = getPrivateField(clientConfiguration, "attributes", AttributeMap.class);
        assertThat(attributes.get(SYNC_HTTP_CLIENT)).isExactlyInstanceOf(MockSdkHttpClient.class);
        assertThat(attributes.get(CONFIGURED_SYNC_HTTP_CLIENT_BUILDER)).isExactlyInstanceOf(MockSdkHttpClientBuilder.class);
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

    private static <T> T getPrivateField(Object object, String fieldName, Class<T> fieldType) {
        try {
            Field field = object.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return fieldType.cast(field.get(object));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static class MockSdkHttpClientBuilder implements SdkHttpClient.Builder {
        @Override
        public SdkHttpClient buildWithDefaults(AttributeMap attributeMap) {
            return new MockSdkHttpClient();
        }
    }

    private static class MockSdkHttpClient implements SdkHttpClient {
        @Override
        public ExecutableHttpRequest prepareRequest(HttpExecuteRequest httpExecuteRequest) {
            return null;
        }

        public String clientName() {
            return "MockSdkHttpClient";
        }

        @Override
        public void close() {

        }
    }
}
