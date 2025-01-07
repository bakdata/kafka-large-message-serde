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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.stream.IntStream;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.VersioningConfiguration;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class AmazonS3ClientIntegrationTest extends AmazonS3IntegrationTest {

    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();

    private static byte[] serialize(final String s) {
        return STRING_SERIALIZER.serialize(null, s);
    }

    @Test
    void shouldReadBackedText() {
        final String bucket = "bucket";
        final S3Client s3 = this.getS3Client();
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        final String key = "key";
        this.store(bucket, key, "foo");
        try (final BlobStorageClient client = new AmazonS3Client(s3)) {
            assertThat(client.getObject(bucket, key))
                    .isEqualTo(serialize("foo"));
        }
    }

    @Test
    void shouldWriteBackedText() {
        final String bucket = "bucket";
        final String key = "key";
        final S3Client s3 = this.getS3Client();
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        try (final BlobStorageClient client = new AmazonS3Client(s3)) {
            assertThat(client.putObject(serialize("foo"), bucket, key))
                    .isEqualTo("s3://bucket/key");
        }
    }

    @Test
    void shouldDeleteFiles() {
        final String bucket = "bucket";
        final S3Client s3 = this.getS3Client();
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        try (final BlobStorageClient client = new AmazonS3Client(s3)) {
            client.putObject(serialize("foo"), bucket, "base/foo/1");
            client.putObject(serialize("foo"), bucket, "base/foo/2");
            client.putObject(serialize("foo"), bucket, "base/bar/1");
            final ListObjectsRequest request = ListObjectsRequest.builder().bucket(bucket).prefix("base/").build();
            assertThat(s3.listObjects(request).contents()).hasSize(3);
            client.deleteAllObjects(bucket, "base/foo/");
        assertThat(s3.listObjects(request).contents()).hasSize(1);
        }
    }

    @Test
    void shouldDeleteManyFiles() {
        final String bucket = "bucket";
        final S3Client s3 = this.getS3Client();
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        try (final BlobStorageClient client = new AmazonS3Client(s3)) {
            IntStream.range(0, 1001)
                    .forEach(i -> client.putObject(serialize("foo"), bucket, "base/foo/" + i));
            final ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucket).prefix("base/").build();
            assertThat(s3.listObjectsV2Paginator(request).contents()).hasSize(1001);
            client.deleteAllObjects(bucket, "base/foo/");
        assertThat(s3.listObjectsV2Paginator(request).contents()).isEmpty();
        }
    }

    @Test
    void shouldDeleteFilesIfEmpty() {
        final String bucket = "bucket";
        final S3Client s3 = this.getS3Client();
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        try (final BlobStorageClient client = new AmazonS3Client(s3)) {
            client.putObject(serialize("foo"), bucket, "base/bar/1");
            final ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucket).prefix("base/").build();
            assertThat(s3.listObjectsV2Paginator(request).contents()).hasSize(1);
            client.deleteAllObjects(bucket, "base/foo/");
        assertThat(s3.listObjectsV2Paginator(request).contents()).hasSize(1);
        }
    }

    @Test
    void shouldDeleteVersions() {
        final String bucket = "bucket";
        final S3Client s3 = this.getS3Client();
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        s3.putBucketVersioning(PutBucketVersioningRequest.builder()
                .bucket(bucket)
                .versioningConfiguration(VersioningConfiguration.builder()
                        .status(BucketVersioningStatus.ENABLED)
                        .build())
                .build());
        try (final BlobStorageClient client = new AmazonS3Client(s3)) {
            client.putObject(serialize("foo"), bucket, "base/foo/1");
            client.putObject(serialize("foo"), bucket, "base/foo/1");
            client.putObject(serialize("foo"), bucket, "base/bar/1");
            client.putObject(serialize("foo"), bucket, "base/bar/1");
            final ListObjectVersionsRequest request =
                    ListObjectVersionsRequest.builder().bucket(bucket).prefix("base/").build();
            assertThat(s3.listObjectVersionsPaginator(request).versions()).hasSize(4);
            client.deleteAllObjects(bucket, "base/foo/");
        assertThat(s3.listObjectVersionsPaginator(request).versions()).hasSize(2);
        }
    }

    @Test
    void shouldDeleteManyVersions() {
        final String bucket = "bucket";
        final S3Client s3 = this.getS3Client();
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        s3.putBucketVersioning(PutBucketVersioningRequest.builder()
                .bucket(bucket)
                .versioningConfiguration(VersioningConfiguration.builder()
                        .status(BucketVersioningStatus.ENABLED)
                        .build())
                .build());
        try (final BlobStorageClient client = new AmazonS3Client(s3)) {
            IntStream.range(0, 1001)
                    .forEach(i -> client.putObject(serialize("foo"), bucket, "base/foo/1"));
            final ListObjectVersionsRequest request =
                    ListObjectVersionsRequest.builder().bucket(bucket).prefix("base/").build();
            assertThat(s3.listObjectVersionsPaginator(request).versions()).hasSize(1001);
            client.deleteAllObjects(bucket, "base/foo/");
        assertThat(s3.listObjectVersionsPaginator(request).versions()).isEmpty();
        }
    }

    @Test
    void shouldThrowExceptionOnMissingObject() {
        final String bucket = "bucket";
        final S3Client s3 = this.getS3Client();
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        final String key = "key";
        try (final BlobStorageClient client = new AmazonS3Client(s3)) {
            assertThatExceptionOfType(SerializationException.class)
                    .isThrownBy(() -> client.getObject(bucket, key))
                    .withMessageStartingWith("Cannot handle S3 backed message:")
                    .withMessageContainingAll(bucket, key)
                    .satisfies(e -> assertThat(e.getCause())
                            .isInstanceOf(NoSuchKeyException.class)
                            .hasMessageContaining("The specified key does not exist.")
                    );
        }
    }

    @Test
    void shouldThrowExceptionOnGetError() {
        final String bucket = "bucket";
        final String key = "key";
        final S3Client s3 = mock(S3Client.class);
        when(s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build())).then(
                (Answer<ResponseInputStream<GetObjectResponse>>) invocation -> {
                    throw new FakeSdkException();
                });
        try (final BlobStorageClient client = new AmazonS3Client(s3)) {
            assertThatExceptionOfType(SerializationException.class)
                    .isThrownBy(() -> client.getObject(bucket, key))
                    .withMessageStartingWith("Cannot handle S3 backed message:")
                    .withMessageContainingAll(bucket, key)
                    .withCauseInstanceOf(SdkException.class);
        }
    }

    @Test
    void shouldThrowExceptionOnPutError() {
        final String bucket = "bucket";
        final String key = "key";
        final S3Client s3 = mock(S3Client.class);
        when(s3.putObject(ArgumentMatchers.<PutObjectRequest>any(), ArgumentMatchers.<RequestBody>any())).then(
                (Answer<ResponseInputStream<GetObjectResponse>>) invocation -> {
                    throw new FakeSdkException();
                });
        try (final BlobStorageClient client = new AmazonS3Client(s3)) {
            final byte[] foo = serialize("foo");
            assertThatExceptionOfType(SerializationException.class)
                    .isThrownBy(() -> client.putObject(foo, bucket, key))
                    .withMessageStartingWith("Error backing message on S3")
                    .withCauseInstanceOf(FakeSdkException.class);
        }
    }

    @Test
    void shouldThrowExceptionOnMissingBucketForGet() {
        final String bucket = "bucket";
        final String key = "key";
        final S3Client s3 = this.getS3Client();
        try (final BlobStorageClient client = new AmazonS3Client(s3)) {
            assertThatExceptionOfType(SerializationException.class)
                    .isThrownBy(() -> client.getObject(bucket, key))
                    .withMessageStartingWith("Cannot handle S3 backed message:")
                    .withMessageContainingAll(bucket, key)
                    .satisfies(e -> assertThat(e.getCause())
                            .isInstanceOf(NoSuchBucketException.class)
                            .hasMessageContaining("The specified bucket does not exist")
                    );
        }
    }

    @Test
    void shouldThrowExceptionOnMissingBucketForPut() {
        final String bucket = "bucket";
        final String key = "key";
        final S3Client s3 = this.getS3Client();
        try (final BlobStorageClient client = new AmazonS3Client(s3)) {
            final byte[] foo = serialize("foo");
            assertThatExceptionOfType(SerializationException.class)
                    .isThrownBy(() -> client.putObject(foo, bucket, key))
                    .withMessageStartingWith("Error backing message on S3")
                    .satisfies(e -> assertThat(e.getCause())
                            .isInstanceOf(NoSuchBucketException.class)
                            .hasMessageContaining("The specified bucket does not exist")
                    );
        }
    }

    private void store(final String bucket, final String key, final String s) {
        final S3Client s3 = this.getS3Client();
        s3.putObject(PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build(), RequestBody.fromString(s));
    }

    private static final class FakeSdkException extends SdkException {

        private FakeSdkException() {
            super(builder());
        }
    }

}
