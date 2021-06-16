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

import static com.bakdata.kafka.LargeMessageStoringClient.serialize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.collect.ImmutableMap;
import io.confluent.common.config.ConfigDef;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class LargeMessageRetrievingClientTest {

    @RegisterExtension
    static final S3MockExtension S3_MOCK = S3MockExtension.builder().silent()
            .withSecureConnection(false).build();
    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();

    private static Map<String, Object> createProperties() {
        return ImmutableMap.<String, Object>builder()
                .put(AbstractLargeMessageConfig.S3_ENDPOINT_CONFIG, "http://localhost:" + S3_MOCK.getHttpPort())
                .put(AbstractLargeMessageConfig.S3_REGION_CONFIG, "us-east-1")
                .put(AbstractLargeMessageConfig.S3_ACCESS_KEY_CONFIG, "foo")
                .put(AbstractLargeMessageConfig.S3_SECRET_KEY_CONFIG, "bar")
                .put(AbstractLargeMessageConfig.S3_ENABLE_PATH_STYLE_ACCESS_CONFIG, true)
                .build();
    }

    private static void store(final String bucket, final String key, final String s) {
        S3_MOCK.createS3Client().putObject(bucket, key, new ByteArrayInputStream(s.getBytes()), new ObjectMetadata());
    }

    private static byte[] createNonBackedText(final String text) {
        return serialize(STRING_SERIALIZER.serialize(null, text));
    }

    private static byte[] createBackedText(final String bucket, final String key) {
        final String uri = "s3://" + bucket + "/" + key;
        return serialize(uri);
    }

    private static LargeMessageRetrievingClient createRetriever() {
        final Map<String, Object> properties = createProperties();
        final ConfigDef configDef = AbstractLargeMessageConfig.baseConfigDef();
        final AbstractLargeMessageConfig config = new AbstractLargeMessageConfig(configDef, properties);
        return config.getRetriever();
    }

    @Test
    void shouldReadNonBackedText() {
        final LargeMessageRetrievingClient retriever = createRetriever();
        assertThat(retriever.retrieveBytes(createNonBackedText("foo")))
                .isEqualTo(STRING_SERIALIZER.serialize(null, "foo"));
    }

    @Test
    void shouldReadNull() {
        final LargeMessageRetrievingClient retriever = createRetriever();
        assertThat(retriever.retrieveBytes(null))
                .isNull();
    }

    @Test
    void shouldReadBackedText() {
        final String bucket = "bucket";
        S3_MOCK.createS3Client().createBucket(bucket);
        final String key = "key";
        store(bucket, key, "foo");
        final LargeMessageRetrievingClient retriever = createRetriever();
        assertThat(retriever.retrieveBytes(createBackedText(bucket, key)))
                .isEqualTo(STRING_SERIALIZER.serialize(null, "foo"));
        S3_MOCK.createS3Client().deleteBucket(bucket);
    }

    @Test
    void shouldThrowExceptionOnErroneousFlag() {
        final LargeMessageRetrievingClient retriever = createRetriever();
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> retriever.retrieveBytes(new byte[]{2}))
                .withMessage("Message can only be marked as backed or non-backed");
    }

    @Test
    void shouldThrowExceptionOnS3Error() {
        final String bucket = "bucket";
        final String key = "key";
        final AmazonS3 s3 = mock(AmazonS3.class);
        when(s3.getObject(bucket, key)).then((Answer<S3Object>) invocation -> {
            throw new IOException();
        });
        final Map<String, Supplier<BlobStorageClient>> clients =
                ImmutableMap.of(AmazonS3Client.SCHEME, () -> new AmazonS3Client(s3));
        final LargeMessageRetrievingClient retriever = new LargeMessageRetrievingClient(clients);
        assertThatExceptionOfType(SerializationException.class)
                .isThrownBy(() -> retriever.retrieveBytes(createBackedText(bucket, key)))
                .withMessageStartingWith("Cannot handle S3 backed message:")
                .withMessageContainingAll(bucket, key)
                .withCauseInstanceOf(IOException.class);
    }

    @Test
    void shouldThrowExceptionOnMissingObject() {
        final String bucket = "bucket";
        S3_MOCK.createS3Client().createBucket(bucket);
        final String key = "key";
        final LargeMessageRetrievingClient retriever = createRetriever();
        assertThatExceptionOfType(AmazonS3Exception.class)
                .isThrownBy(() -> retriever.retrieveBytes(createBackedText(bucket, key)))
                .withMessageStartingWith("The specified key does not exist.");
        S3_MOCK.createS3Client().deleteBucket(bucket);
    }

    @Test
    void shouldThrowExceptionOnMissingBucket() {
        final String bucket = "bucket";
        final String key = "key";
        final LargeMessageRetrievingClient retriever = createRetriever();
        assertThatExceptionOfType(AmazonS3Exception.class)
                .isThrownBy(() -> retriever.retrieveBytes(createBackedText(bucket, key)))
                .withMessageStartingWith("The specified bucket does not exist.");
    }

}