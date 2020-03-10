/*
 * MIT License
 *
 * Copyright (c) 2020 bakdata
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

import static com.bakdata.kafka.S3BackedStoringClient.CHARSET;
import static com.bakdata.kafka.S3BackedStoringClient.IS_BACKED;
import static com.bakdata.kafka.S3BackedStoringClient.IS_NOT_BACKED;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;

/**
 * Client for retrieving actual bytes of messages stored with {@link S3BackedStoringClient}.
 */
@Slf4j
@RequiredArgsConstructor
class S3BackedRetrievingClient {

    private final @NonNull AmazonS3 s3;

    static AmazonS3URI deserializeUri(final byte[] data) {
        final byte[] uriBytes = getBytes(data);
        final String uri = new String(uriBytes, CHARSET);
        return new AmazonS3URI(uri);
    }

    static byte[] getBytes(final byte[] data) {
        final byte[] bytes = new byte[data.length - 1];
        // flag is stored in first byte
        System.arraycopy(data, 1, bytes, 0, data.length - 1);
        return bytes;
    }

    byte[] retrieveBytes(final byte[] data) {
        if (data == null) {
            return null;
        }
        if (data[0] == IS_NOT_BACKED) {
            return getBytes(data);
        }
        if (data[0] != IS_BACKED) {
            throw new IllegalArgumentException("Message can only be marked as backed or non-backed");
        }
        return this.retrieveBackedBytes(data);
    }

    private byte[] retrieveBackedBytes(final byte[] data) {
        Objects.requireNonNull(this.s3);
        final AmazonS3URI s3URI = deserializeUri(data);
        try (final S3Object s3Object = this.s3.getObject(s3URI.getBucket(), s3URI.getKey());
                final InputStream in = s3Object.getObjectContent()) {
            final byte[] bytes = IOUtils.toByteArray(in);
            log.debug("Extracted large message from S3: {}", s3URI);
            return bytes;
        } catch (final IOException e) {
            throw new SerializationException("Cannot handle S3 backed message: " + s3URI, e);
        }
    }
}
