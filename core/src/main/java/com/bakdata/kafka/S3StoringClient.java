package com.bakdata.kafka;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ObjectMetadata;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;

/**
 * Client for storing large {@code byte[]} on Amazon S3 if the size exceeds a defined limit.
 */
@Slf4j
@Builder
class S3StoringClient {

    static final byte IS_NOT_BACKED = 0;
    static final byte IS_BACKED = 1;
    static final Charset CHARSET = StandardCharsets.UTF_8;
    private static final String VALUE_PREFIX = "values";
    private static final String KEY_PREFIX = "keys";
    private final @NonNull AmazonS3 s3;
    private final AmazonS3URI basePath;
    private final int maxSize;

    private static String toString(final String s) {
        return s == null ? "" : s;
    }

    static byte[] serialize(final String uri) {
        final byte[] uriBytes = uri.getBytes(CHARSET);
        return serialize(uriBytes, IS_BACKED);
    }

    static byte[] serialize(final byte[] bytes) {
        return serialize(bytes, IS_NOT_BACKED);
    }

    private static byte[] serialize(final byte[] bytes, final byte flag) {
        final byte[] fullBytes = new byte[bytes.length + 1];
        fullBytes[0] = flag;
        System.arraycopy(bytes, 0, fullBytes, 1, bytes.length);
        return fullBytes;
    }

    private static ObjectMetadata createMetadata(final byte[] bytes) {
        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(bytes.length);
        return metadata;
    }

    private String createS3Key(final String topic, final boolean isKey) {
        final String prefix = isKey ? KEY_PREFIX : VALUE_PREFIX;
        return toString(this.basePath.getKey()) + topic + "/" + prefix + "/" + UUID.randomUUID();
    }

    private boolean needsS3Backing(final byte[] bytes) {
        return bytes.length >= this.maxSize;
    }

    byte[] storeBytes(final String topic, final byte[] bytes, final boolean isKey) {
        if (this.needsS3Backing(bytes)) {
            final String key = this.createS3Key(topic, isKey);
            final String uri = this.uploadToS3(key, bytes);
            return serialize(uri);
        } else {
            return serialize(bytes);
        }
    }

    private String uploadToS3(final String key, final byte[] bytes) {
        Objects.requireNonNull(this.basePath);
        final String bucket = this.basePath.getBucket();
        try (final InputStream content = new ByteArrayInputStream(bytes)) {
            final ObjectMetadata metadata = createMetadata(bytes);
            this.s3.putObject(bucket, key, content, metadata);
            final String uri = "s3://" + bucket + "/" + key;
            log.debug("Stored large message on S3: {}", uri);
            return uri;
        } catch (final IOException e) {
            throw new SerializationException("Error backing message on S3", e);
        }
    }
}
