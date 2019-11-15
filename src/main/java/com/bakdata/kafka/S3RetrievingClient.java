package com.bakdata.kafka;

import static com.bakdata.kafka.S3StoringClient.CHARSET;
import static com.bakdata.kafka.S3StoringClient.IS_BACKED;
import static com.bakdata.kafka.S3StoringClient.IS_NOT_BACKED;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;

/**
 * Client for retrieving actual bytes of messages stored with {@link S3StoringClient}.
 */
@Slf4j
@RequiredArgsConstructor
class S3RetrievingClient {

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

    byte[] retrieveBytes(final byte[] data) {
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
}
