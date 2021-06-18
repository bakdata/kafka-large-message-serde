/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata
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

import static com.bakdata.kafka.LargeMessageRetrievingClient.deserializeUri;
import static com.bakdata.kafka.LargeMessageRetrievingClient.getBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.IOUtils;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class LargeMessageStoringClientS3IntegrationTest {

    @RegisterExtension
    static final S3MockExtension S3_MOCK = S3MockExtension.builder().silent()
            .withSecureConnection(false).build();
    private static final String TOPIC = "output";
    private static final Deserializer<String> STRING_DESERIALIZER = Serdes.String().deserializer();
    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();
    @Mock
    static IdGenerator idGenerator;

    private static Map<String, Object> createProperties(final Map<String, Object> properties) {
        return ImmutableMap.<String, Object>builder()
                .putAll(properties)
                .put(AbstractLargeMessageConfig.S3_ENDPOINT_CONFIG, "http://localhost:" + S3_MOCK.getHttpPort())
                .put(AbstractLargeMessageConfig.S3_REGION_CONFIG, "us-east-1")
                .put(AbstractLargeMessageConfig.S3_ACCESS_KEY_CONFIG, "foo")
                .put(AbstractLargeMessageConfig.S3_SECRET_KEY_CONFIG, "bar")
                .put(AbstractLargeMessageConfig.S3_ENABLE_PATH_STYLE_ACCESS_CONFIG, true)
                .build();
    }

    private static void expectNonBackedText(final String expected, final byte[] backedText) {
        assertThat(STRING_DESERIALIZER.deserialize(null, getBytes(backedText)))
                .isInstanceOf(String.class)
                .isEqualTo(expected);
    }

    private static void expectBackedText(final String basePath, final String expected, final byte[] backedText,
            final String type) {
        final BlobStorageURI uri = deserializeUri(backedText);
        expectBackedText(basePath, expected, uri, type);
    }

    private static void expectBackedText(final String basePath, final String expected, final BlobStorageURI uri,
            final String type) {
        assertThat(uri).asString().startsWith(basePath + TOPIC + "/" + type + "/");
        final byte[] bytes = readBytes(uri);
        final String deserialized = STRING_DESERIALIZER.deserialize(null, bytes);
        assertThat(deserialized).isEqualTo(expected);
    }

    private static byte[] readBytes(final BlobStorageURI uri) {
        try (final S3Object object = S3_MOCK.createS3Client().getObject(uri.getBucket(), uri.getKey());
                final S3ObjectInputStream objectContent = object.getObjectContent()) {
            return IOUtils.toByteArray(objectContent);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static LargeMessageStoringClient createStorer(final Map<String, Object> baseProperties) {
        final Map<String, Object> properties = createProperties(baseProperties);
        final AbstractLargeMessageConfig config = new AbstractLargeMessageConfig(properties);
        return config.getStorer();
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
        final LargeMessageStoringClient storer = createStorer(properties);
        assertThat(storer.storeBytes(null, serialize("foo"), isKey))
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
        final AmazonS3 s3 = S3_MOCK.createS3Client();
        s3.createBucket(bucket);
        final LargeMessageStoringClient storer = createStorer(properties);
        assertThat(storer.storeBytes(TOPIC, serialize("foo"), true))
                .satisfies(backedText -> expectBackedText(basePath, "foo", backedText, "keys"));
        s3.deleteBucket(bucket);
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
        final AmazonS3 s3 = S3_MOCK.createS3Client();
        s3.createBucket(bucket);
        final LargeMessageStoringClient storer = createStorer(properties);
        when(idGenerator.generateId("foo".getBytes())).thenReturn("bar");
        assertThat(storer.storeBytes(TOPIC, serialize("foo"), true))
                .satisfies(backedText -> {
                    final BlobStorageURI uri = deserializeUri(backedText);
                    expectBackedText(basePath, "foo", uri, "keys");
                    assertThat(uri).asString().endsWith("bar");
                });
        s3.deleteBucket(bucket);
    }

    public static class MockIdGenerator implements IdGenerator {
        @Override
        public String generateId(final byte[] bytes) {
            return idGenerator.generateId(bytes);
        }
    }
}