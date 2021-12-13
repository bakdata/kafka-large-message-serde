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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.util.IOUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;

/**
 * Implementation of {@link BlobStorageClient} for Amazon S3.
 */
@Slf4j
@RequiredArgsConstructor
class AmazonS3Client implements BlobStorageClient {

    static final String SCHEME = "s3";
    private final @NonNull AmazonS3 s3;

    private static ObjectMetadata createMetadata(final byte[] bytes) {
        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(bytes.length);
        return metadata;
    }

    private static String asURI(final String bucket, final String key) {
        return SCHEME + "://" + bucket + "/" + key;
    }

    @Override
    public void deleteAllObjects(final String bucket, final String prefix) {
        // https://docs.aws.amazon.com/AmazonS3/latest/userguide/delete-bucket.html#delete-empty-bucket
        this.deleteObjects(bucket, prefix);
        this.deleteVersions(bucket, prefix);
    }

    @Override
    public String putObject(final byte[] bytes, final String bucket, final String key) {
        try (final InputStream content = new ByteArrayInputStream(bytes)) {
            final ObjectMetadata metadata = createMetadata(bytes);
            this.s3.putObject(bucket, key, content, metadata);
            return asURI(bucket, key);
        } catch (final IOException e) {
            throw new SerializationException("Error backing message on S3", e);
        }
    }

    @Override
    public byte[] getObject(final String bucket, final String key) {
        final String s3URI = asURI(bucket, key);
        try (final S3Object s3Object = this.s3.getObject(bucket, key);
                final InputStream in = s3Object.getObjectContent()) {
            return IOUtils.toByteArray(in);
        } catch (final IOException e) {
            throw new SerializationException("Cannot handle S3 backed message: " + s3URI, e);
        }
    }

    private void deleteObjects(final String bucketName, final String prefix) {
        ObjectListing objectListing = this.s3.listObjects(bucketName, prefix);
        while (true) {
            final String[] keys = objectListing.getObjectSummaries().stream()
                    .map(S3ObjectSummary::getKey)
                    .toArray(String[]::new);
            this.s3.deleteObjects(new DeleteObjectsRequest(bucketName)
                    .withKeys(keys));

            // If the bucket contains many objects, the listObjects() call
            // might not return all of the objects in the first listing. Check to
            // see whether the listing was truncated. If so, retrieve the next page of objects
            // and delete them.
            if (objectListing.isTruncated()) {
                objectListing = this.s3.listNextBatchOfObjects(objectListing);
            } else {
                break;
            }
        }
    }

    private void deleteVersions(final String bucketName, final String prefix) {
        // Delete all object versions (required for versioned buckets).
        VersionListing versionListing = this.s3.listVersions(bucketName, prefix);
        while (true) {
            for (final S3VersionSummary versionSummary : versionListing.getVersionSummaries()) {
                this.s3.deleteVersion(bucketName, versionSummary.getKey(), versionSummary.getVersionId());
            }

            if (versionListing.isTruncated()) {
                versionListing = this.s3.listNextBatchOfVersions(versionListing);
            } else {
                break;
            }
        }
    }
}
