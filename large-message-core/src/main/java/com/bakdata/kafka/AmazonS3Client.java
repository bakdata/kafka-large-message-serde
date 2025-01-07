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

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.ObjectVersion;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectVersionsIterable;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

/**
 * Implementation of {@link BlobStorageClient} for Amazon S3.
 */
@Slf4j
@RequiredArgsConstructor
class AmazonS3Client implements BlobStorageClient {

    static final String SCHEME = "s3";
    private final @NonNull S3Client s3;

    static ObjectIdentifier asIdentifier(final S3Object s3Object) {
        return ObjectIdentifier.builder()
                .key(s3Object.key())
                .build();
    }

    private static ObjectIdentifier asIdentifier(final ObjectVersion s3Object) {
        return ObjectIdentifier.builder()
                .key(s3Object.key())
                .versionId(s3Object.versionId())
                .build();
    }

    private static String asURI(final String bucket, final String key) {
        return SCHEME + "://" + bucket + "/" + key;
    }

    private static List<ObjectIdentifier> asIdentifiers(final ListObjectsV2Response response) {
        return response.contents().stream()
                .map(AmazonS3Client::asIdentifier)
                .collect(Collectors.toList());
    }

    private static List<ObjectIdentifier> asIdentifiers(final ListObjectVersionsResponse response) {
        return response.versions().stream()
                .map(AmazonS3Client::asIdentifier)
                .collect(Collectors.toList());
    }

    @Override
    public void deleteAllObjects(final String bucket, final String prefix) {
        // https://docs.aws.amazon.com/AmazonS3/latest/userguide/delete-bucket.html#delete-empty-bucket
        this.deleteObjects(bucket, prefix);
        this.deleteVersions(bucket, prefix);
    }

    @Override
    public String putObject(final byte[] bytes, final String bucket, final String key) {
        try {
            final PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            this.s3.putObject(request, RequestBody.fromBytes(bytes));
            return asURI(bucket, key);
        } catch (final SdkException e) {
            throw new SerializationException("Error backing message on S3", e);
        }
    }

    @Override
    public byte[] getObject(final String bucket, final String key) {
        final String s3URI = asURI(bucket, key);
        final GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        try (final InputStream s3Object = this.s3.getObject(request)) {
            return s3Object.readAllBytes();
        } catch (final SdkException | IOException e) {
            throw new SerializationException("Cannot handle S3 backed message: " + s3URI, e);
        }
    }

    @Override
    public void close() {
        this.s3.close();
    }

    private void deleteObjects(final String bucketName, final String prefix) {
        final ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(prefix)
                .build();
        final ListObjectsV2Iterable objectListing = this.s3.listObjectsV2Paginator(request);
        objectListing.stream()
                .map(AmazonS3Client::asIdentifiers)
                .forEach(keys -> this.delete(bucketName, keys));
    }

    private void deleteVersions(final String bucketName, final String prefix) {
        // Delete all object versions (required for versioned buckets).
        final ListObjectVersionsRequest request = ListObjectVersionsRequest.builder()
                .bucket(bucketName)
                .prefix(prefix)
                .build();
        final ListObjectVersionsIterable versionListing = this.s3.listObjectVersionsPaginator(request);
        versionListing.stream()
                .map(AmazonS3Client::asIdentifiers)
                .forEach(keys -> this.delete(bucketName, keys));
    }

    private void delete(final String bucketName, final Collection<ObjectIdentifier> keys) {
        if (!keys.isEmpty()) {
            this.s3.deleteObjects(DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(Delete.builder()
                            .objects(keys)
                            .build())
                    .build());
        }
    }
}
