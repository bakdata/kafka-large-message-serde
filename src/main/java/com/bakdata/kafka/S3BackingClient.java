package com.bakdata.kafka;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;

@Slf4j
@RequiredArgsConstructor
class S3BackingClient {

    private static final byte IS_NOT_BACKED = 0;
    private static final byte IS_BACKED = 1;
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private final @NonNull AmazonS3 s3;

    static String deserializeUri(final byte[] data) {
        final byte[] uriBytes = getBytes(data);
        return new String(uriBytes, CHARSET);
    }

    static byte[] getBytes(final byte[] data) {
        final byte[] bytes = new byte[data.length - 1];
        System.arraycopy(data, 1, bytes, 0, data.length - 1);
        return bytes;
    }

    static byte[] serialize(final String uri) {
        final byte[] uriBytes = uri.getBytes(CHARSET);
        return serialize(uriBytes, IS_BACKED);
    }

    static byte[] serialize(final byte[] bytes) {
        return serialize(bytes, IS_NOT_BACKED);
    }

    private static byte[] serialize(final byte[] uriBytes, final byte flag) {
        final byte[] fullBytes = prepareBytes(uriBytes);
        fullBytes[0] = flag;
        return fullBytes;
    }

    private static byte[] prepareBytes(final byte[] bytes) {
        final byte[] fullBytes = new byte[bytes.length + 1];
        System.arraycopy(bytes, 0, fullBytes, 1, bytes.length);
        return fullBytes;
    }

    byte[] extractBytes(final byte[] data) {
        if (data[0] == IS_NOT_BACKED) {
            return getBytes(data);
        }
        if (data[0] != IS_BACKED) {
            throw new IllegalArgumentException("Message can only be marked as backed or non-backed");
        }
        Objects.requireNonNull(this.s3);
        final String uri = deserializeUri(data);
        final AmazonS3URI s3URI = new AmazonS3URI(uri);
        try (final S3Object s3Object = this.s3.getObject(s3URI.getBucket(), s3URI.getKey());
                final InputStream in = s3Object.getObjectContent()) {
            final byte[] bytes = in.readAllBytes();
            log.info("Extracted large message from S3: {}", uri);
            return bytes;
        } catch (final IOException e) {
            throw new SerializationException("Cannot handle S3 backed message: " + s3URI, e);
        }
    }

    String uploadToS3(final String bucket, final String key, final byte[] bytes) {
        try (final InputStream content = new ByteArrayInputStream(bytes)) {
            final ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(bytes.length);
            this.s3.putObject(bucket, key, content, metadata);
            final String uri = "s3://" + bucket + "/" + key;
            log.info("Stored large message on S3: {}", uri);
            return uri;
        } catch (final IOException e) {
            throw new SerializationException("Error backing message on S3", e);
        }
    }
}
