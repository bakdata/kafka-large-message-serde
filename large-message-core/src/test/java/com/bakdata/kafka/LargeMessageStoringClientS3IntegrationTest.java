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

import static com.bakdata.kafka.ByteFlagLargeMessagePayloadProtocol.stripFlag;
import static com.bakdata.kafka.LargeMessageRetrievingClient.deserializeUri;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.utils.IoUtils;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class LargeMessageStoringClientS3IntegrationTest extends AmazonS3IntegrationTest {

    private static final String TOPIC = "output";
    private static final Deserializer<String> STRING_DESERIALIZER = Serdes.String().deserializer();
    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();
    @Mock
    private static IdGenerator idGenerator;

    static BlobStorageURI deserializeUriWithFlag(final byte[] data) {
        final byte[] uriBytes = stripFlag(data);
        return deserializeUri(uriBytes);
    }

    private static void expectNonBackedText(final String expected, final byte[] backedText) {
        assertThat(STRING_DESERIALIZER.deserialize(null, stripFlag(backedText)))
                .isInstanceOf(String.class)
                .isEqualTo(expected);
    }

    private static byte[] serialize(final String s) {
        return STRING_SERIALIZER.serialize(null, s);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldWriteNonBackedText(final boolean isKey) {
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, Integer.MAX_VALUE)
                .build();
        final LargeMessageStoringClient storer = this.createStorer(properties);
        assertThat(storer.storeBytes(null, serialize("foo"), isKey, new RecordHeaders()))
                .satisfies(backedText -> expectNonBackedText("foo", backedText));
    }

    @Test
    void shouldWriteBackedTextKey() {
        final String bucket = "bucket";
        final String basePath = "s3://" + bucket + "/base/";
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, 0)
                .put(AbstractLargeMessageConfig.BASE_PATH_CONFIG, basePath)
                .build();
        final S3Client s3 = this.getS3Client();
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        final LargeMessageStoringClient storer = this.createStorer(properties);
        assertThat(storer.storeBytes(TOPIC, serialize("foo"), true, new RecordHeaders()))
                .satisfies(backedText -> this.expectBackedText(basePath, "foo", backedText, "keys"));
    }

    @Test
    void shouldUseConfiguredIdGenerator() {
        final String bucket = "bucket";
        final String basePath = "s3://" + bucket + "/base/";
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, 0)
                .put(AbstractLargeMessageConfig.BASE_PATH_CONFIG, basePath)
                .put(AbstractLargeMessageConfig.ID_GENERATOR_CONFIG, MockIdGenerator.class)
                .build();
        final S3Client s3 = this.getS3Client();
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        final LargeMessageStoringClient storer = this.createStorer(properties);
        when(idGenerator.generateId("foo".getBytes())).thenReturn("bar");
        assertThat(storer.storeBytes(TOPIC, serialize("foo"), true, new RecordHeaders()))
                .satisfies(backedText -> {
                    final BlobStorageURI uri = deserializeUriWithFlag(backedText);
                    this.expectBackedText(basePath, "foo", uri, "keys");
                    assertThat(uri).asString().endsWith("bar");
                });
    }

    private Map<String, Object> createProperties(final Map<String, Object> properties) {
        final AwsBasicCredentials credentials = this.getCredentials();
        return ImmutableMap.<String, Object>builder()
                .putAll(properties)
                .put(AbstractLargeMessageConfig.S3_ENDPOINT_CONFIG, this.getEndpointOverride().toString())
                .put(AbstractLargeMessageConfig.S3_REGION_CONFIG, this.getRegion().id())
                .put(AbstractLargeMessageConfig.S3_ACCESS_KEY_CONFIG, credentials.accessKeyId())
                .put(AbstractLargeMessageConfig.S3_SECRET_KEY_CONFIG, credentials.secretAccessKey())
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
        try (final ResponseInputStream<GetObjectResponse> objectContent = this.getS3Client().getObject(
                GetObjectRequest.builder()
                        .bucket(uri.getBucket())
                        .key(uri.getKey())
                        .build())) {
            return IoUtils.toByteArray(objectContent);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private LargeMessageStoringClient createStorer(final Map<String, Object> baseProperties) {
        final Map<String, Object> properties = this.createProperties(baseProperties);
        final AbstractLargeMessageConfig config = new AbstractLargeMessageConfig(properties);
        return config.getStorer();
    }

    public static class MockIdGenerator implements IdGenerator {
        @Override
        public String generateId(final byte[] bytes) {
            return idGenerator.generateId(bytes);
        }
    }
}
