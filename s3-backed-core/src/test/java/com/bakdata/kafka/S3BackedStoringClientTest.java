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

import static com.bakdata.kafka.S3BackedRetrievingClient.deserializeUri;
import static com.bakdata.kafka.S3BackedRetrievingClient.getBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.IOUtils;
import com.google.common.collect.ImmutableMap;
import io.confluent.common.config.ConfigDef;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.stubbing.Answer;

class S3BackedStoringClientTest {

    @RegisterExtension
    static final S3MockExtension S3_MOCK = S3MockExtension.builder().silent()
            .withSecureConnection(false).build();
    private static final String TOPIC = "output";
    private static final Deserializer<String> STRING_DESERIALIZER = Serdes.String().deserializer();
    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();

    private static Map<String, Object> createProperties(final Map<String, Object> properties) {
        return ImmutableMap.<String, Object>builder()
                .putAll(properties)
                .put(AbstractS3BackedConfig.S3_ENDPOINT_CONFIG, "http://localhost:" + S3_MOCK.getHttpPort())
                .put(AbstractS3BackedConfig.S3_REGION_CONFIG, "us-east-1")
                .put(AbstractS3BackedConfig.S3_ACCESS_KEY_CONFIG, "foo")
                .put(AbstractS3BackedConfig.S3_SECRET_KEY_CONFIG, "bar")
                .put(AbstractS3BackedConfig.S3_ENABLE_PATH_STYLE_ACCESS_CONFIG, true)
                .build();
    }

    private static void expectBackedText(final String basePath, final String expected, final byte[] s3BackedText,
            final String type) {
        final String uri = deserializeUri(s3BackedText);
        assertThat(uri).startsWith(basePath + TOPIC + "/" + type + "/");
        final AmazonS3URI amazonS3URI = new AmazonS3URI(uri);
        final byte[] bytes = readBytes(amazonS3URI);
        final String deserialized = STRING_DESERIALIZER
                .deserialize(null, bytes);
        assertThat(deserialized).isEqualTo(expected);
    }

    private static byte[] readBytes(final AmazonS3URI amazonS3URI) {
        try (final S3Object object = S3_MOCK.createS3Client().getObject(amazonS3URI.getBucket(), amazonS3URI.getKey());
                final S3ObjectInputStream objectContent = object.getObjectContent()) {
            return IOUtils.toByteArray(objectContent);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void expectNonBackedText(final String expected, final byte[] s3BackedText) {
        assertThat(STRING_DESERIALIZER.deserialize(null, getBytes(s3BackedText)))
                .isInstanceOf(String.class)
                .isEqualTo(expected);
    }

    private static S3BackedStoringClient createStorer(final Map<String, Object> baseProperties) {
        final Map<String, Object> properties = createProperties(baseProperties);
        final ConfigDef configDef = AbstractS3BackedConfig.baseConfigDef();
        final AbstractS3BackedConfig config = new AbstractS3BackedConfig(configDef, properties);
        return config.getS3Storer();
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void shouldWriteNonBackedText(final String isKey) {
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractS3BackedConfig.MAX_BYTE_SIZE_CONFIG, Integer.MAX_VALUE)
                .build();
        final S3BackedStoringClient storer = createStorer(properties);
        assertThat(storer.storeBytes(null, STRING_SERIALIZER.serialize(null, "foo"), Boolean.parseBoolean(isKey)))
                .satisfies(s3BackedText -> expectNonBackedText("foo", s3BackedText));
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void shouldWriteNonBackedNull(final String isKey) {
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractS3BackedConfig.MAX_BYTE_SIZE_CONFIG, Integer.MAX_VALUE)
                .build();
        final S3BackedStoringClient storer = createStorer(properties);
        assertThat(storer.storeBytes(null, null, Boolean.parseBoolean(isKey)))
                .isNull();
    }

    @Test
    void shouldWriteBackedTextKey() {
        final String bucket = "bucket";
        final String basePath = "s3://" + bucket + "/base/";
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractS3BackedConfig.MAX_BYTE_SIZE_CONFIG, 0)
                .put(AbstractS3BackedConfig.BASE_PATH_CONFIG, basePath)
                .build();
        final AmazonS3 s3Client = S3_MOCK.createS3Client();
        s3Client.createBucket(bucket);
        final S3BackedStoringClient storer = createStorer(properties);
        assertThat(storer.storeBytes(TOPIC, STRING_SERIALIZER.serialize(null, "foo"), true))
                .satisfies(s3BackedText -> expectBackedText(basePath, "foo", s3BackedText, "keys"));
        s3Client.deleteBucket(bucket);
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void shouldWriteBackedNull(final String isKey) {
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractS3BackedConfig.MAX_BYTE_SIZE_CONFIG, 0)
                .build();
        final S3BackedStoringClient storer = createStorer(properties);
        assertThat(storer.storeBytes(null, null, Boolean.parseBoolean(isKey)))
                .isNull();
    }

    @Test
    void shouldWriteBackedTextValue() {
        final String bucket = "bucket";
        final String basePath = "s3://" + bucket + "/base/";
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractS3BackedConfig.MAX_BYTE_SIZE_CONFIG, 0)
                .put(AbstractS3BackedConfig.BASE_PATH_CONFIG, basePath)
                .build();
        final AmazonS3 s3Client = S3_MOCK.createS3Client();
        s3Client.createBucket(bucket);
        final S3BackedStoringClient storer = createStorer(properties);
        assertThat(storer.storeBytes(TOPIC, STRING_SERIALIZER.serialize(null, "foo"), false))
                .satisfies(s3BackedText -> expectBackedText(basePath, "foo", s3BackedText, "values"));
        s3Client.deleteBucket(bucket);
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void shouldThrowExceptionOnS3Error(final String isKey) {
        final String bucket = "bucket";
        final String basePath = "s3://" + bucket + "/base/";
        final AmazonS3 s3 = mock(AmazonS3.class);
        when(s3.putObject(eq(bucket), any(), any(), any())).then((Answer<S3Object>) invocation -> {
            throw new IOException();
        });
        final S3BackedStoringClient storer = S3BackedStoringClient.builder()
                .s3(s3)
                .basePath(new AmazonS3URI(basePath))
                .maxSize(0)
                .build();
        assertThatExceptionOfType(SerializationException.class)
                .isThrownBy(() -> storer
                        .storeBytes(TOPIC, STRING_SERIALIZER.serialize(null, "foo"), Boolean.parseBoolean(isKey)))
                .withMessageStartingWith("Error backing message on S3")
                .withCauseInstanceOf(IOException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void shouldThrowExceptionOnMissingBucket(final String isKey) {
        final String bucket = "bucket";
        final String basePath = "s3://" + bucket + "/base/";
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractS3BackedConfig.MAX_BYTE_SIZE_CONFIG, 0)
                .put(AbstractS3BackedConfig.BASE_PATH_CONFIG, basePath)
                .build();
        final S3BackedStoringClient storer = createStorer(properties);
        assertThatExceptionOfType(AmazonS3Exception.class)
                .isThrownBy(() -> storer
                        .storeBytes(TOPIC, STRING_SERIALIZER.serialize(null, "foo"), Boolean.parseBoolean(isKey)))
                .withMessageStartingWith("The specified bucket does not exist.");
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void shouldThrowExceptionOnNullTopic(final String isKey) {
        final String bucket = "bucket";
        final String basePath = "s3://" + bucket + "/base/";
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractS3BackedConfig.MAX_BYTE_SIZE_CONFIG, 0)
                .put(AbstractS3BackedConfig.BASE_PATH_CONFIG, basePath)
                .build();
        final S3BackedStoringClient storer = createStorer(properties);
        assertThatNullPointerException()
                .isThrownBy(() -> storer
                        .storeBytes(null, STRING_SERIALIZER.serialize(null, "foo"), Boolean.parseBoolean(isKey)))
                .withMessage("Topic must not be null");
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void shouldThrowExceptionOnNullBasePath(final String isKey) {
        final String bucket = "bucket";
        final String basePath = "s3://" + bucket + "/base/";
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractS3BackedConfig.MAX_BYTE_SIZE_CONFIG, 0)
                .build();
        final S3BackedStoringClient storer = createStorer(properties);
        assertThatNullPointerException()
                .isThrownBy(() -> storer
                        .storeBytes(TOPIC, STRING_SERIALIZER.serialize(null, "foo"), Boolean.parseBoolean(isKey)))
                .withMessage("Base path must not be null");
    }

}