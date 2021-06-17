/*
 * MIT License
 *
 * Copyright (c) 2021 bakdata
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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import java.io.ByteArrayInputStream;
import java.io.IOException;
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
class AmazonS3ClientTest {

    @RegisterExtension
    static final S3MockExtension S3_MOCK = S3MockExtension.builder().silent()
            .withSecureConnection(false).build();
    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();

    private static void store(final String bucket, final String key, final String s) {
        S3_MOCK.createS3Client().putObject(bucket, key, new ByteArrayInputStream(s.getBytes()), new ObjectMetadata());
    }

    private static BlobStorageClient createClient() {
        return new AmazonS3Client(S3_MOCK.createS3Client());
    }

    private static byte[] serialize(final String s) {
        return STRING_SERIALIZER.serialize(null, s);
    }

    @Test
    void shouldReadBackedText() {
        final String bucket = "bucket";
        S3_MOCK.createS3Client().createBucket(bucket);
        final String key = "key";
        store(bucket, key, "foo");
        final BlobStorageClient client = createClient();
        assertThat(client.getObject(bucket, key))
                .isEqualTo(serialize("foo"));
        S3_MOCK.createS3Client().deleteBucket(bucket);
    }

    @Test
    void shouldWriteBackedText() {
        final String bucket = "bucket";
        final String key = "key";
        final AmazonS3 s3Client = S3_MOCK.createS3Client();
        s3Client.createBucket(bucket);
        final BlobStorageClient client = createClient();
        assertThat(client.putObject(serialize("foo"), bucket, key))
                .isEqualTo("s3://bucket/key");
        s3Client.deleteBucket(bucket);
    }

    @Test
    void shouldDeleteFiles() {
        final String bucket = "bucket";
        final AmazonS3 s3Client = S3_MOCK.createS3Client();
        s3Client.createBucket(bucket);
        final BlobStorageClient client = createClient();
        client.putObject(serialize("foo"), bucket, "base/foo/1");
        client.putObject(serialize("foo"), bucket, "base/foo/2");
        client.putObject(serialize("foo"), bucket, "base/bar/1");
        assertThat(s3Client.listObjects(bucket, "base/").getObjectSummaries()).hasSize(3);
        client.deleteAllObjects(bucket, "base/foo/");
        assertThat(s3Client.listObjects(bucket, "base/").getObjectSummaries()).hasSize(1);
        s3Client.deleteBucket(bucket);
    }

    @Test
    void shouldThrowExceptionOnMissingObject() {
        final String bucket = "bucket";
        S3_MOCK.createS3Client().createBucket(bucket);
        final String key = "key";
        final BlobStorageClient client = createClient();
        assertThatExceptionOfType(AmazonS3Exception.class)
                .isThrownBy(() -> client.getObject(bucket, key))
                .withMessageStartingWith("The specified key does not exist.");
        S3_MOCK.createS3Client().deleteBucket(bucket);
    }

    @Test
    void shouldThrowExceptionOnS3GetError() {
        final String bucket = "bucket";
        final String key = "key";
        final AmazonS3 s3 = mock(AmazonS3.class);
        when(s3.getObject(bucket, key)).then((Answer<S3Object>) invocation -> {
            throw new IOException();
        });
        final BlobStorageClient client = new AmazonS3Client(s3);
        assertThatExceptionOfType(SerializationException.class)
                .isThrownBy(() -> client.getObject(bucket, key))
                .withMessageStartingWith("Cannot handle S3 backed message:")
                .withMessageContainingAll(bucket, key)
                .withCauseInstanceOf(IOException.class);
    }

    @Test
    void shouldThrowExceptionOnS3PutError() {
        final String bucket = "bucket";
        final String key = "key";
        final AmazonS3 s3 = mock(AmazonS3.class);
        when(s3.putObject(eq(bucket), any(), any(), any())).then((Answer<S3Object>) invocation -> {
            throw new IOException();
        });
        final BlobStorageClient client = new AmazonS3Client(s3);
        assertThatExceptionOfType(SerializationException.class)
                .isThrownBy(() -> client
                        .putObject(serialize("foo"), bucket, "key"))
                .withMessageStartingWith("Error backing message on S3")
                .withCauseInstanceOf(IOException.class);
    }

    @Test
    void shouldThrowExceptionOnMissingBucketForGet() {
        final String bucket = "bucket";
        final String key = "key";
        final BlobStorageClient client = createClient();
        assertThatExceptionOfType(AmazonS3Exception.class)
                .isThrownBy(() -> client.getObject(bucket, key))
                .withMessageStartingWith("The specified bucket does not exist.");
    }

    @Test
    void shouldThrowExceptionOnMissingBucketForPut() {
        final String bucket = "bucket";
        final String key = "key";
        final BlobStorageClient client = createClient();
        assertThatExceptionOfType(AmazonS3Exception.class)
                .isThrownBy(() -> client.putObject(serialize("foo"), bucket, key))
                .withMessageStartingWith("The specified bucket does not exist.");
    }

}