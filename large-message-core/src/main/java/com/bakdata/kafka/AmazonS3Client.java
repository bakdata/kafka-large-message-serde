package com.bakdata.kafka;

import com.amazonaws.services.s3.AmazonS3;
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

@Slf4j
@RequiredArgsConstructor
public class AmazonS3Client implements BlobStorageClient {

    public static final String SCHEME = "s3";
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
    public void deleteAllFiles(final String bucket, final String prefix) {
        // https://docs.aws.amazon.com/AmazonS3/latest/userguide/delete-bucket.html#delete-empty-bucket
        this.deleteObjects(bucket, prefix);
        this.deleteVersions(bucket, prefix);
    }

    @Override
    public String put(final byte[] bytes, final String bucket, final String key) {
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
            for (final S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                this.s3.deleteObject(bucketName, objectSummary.getKey());
            }

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
