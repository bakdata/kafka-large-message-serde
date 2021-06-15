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

import static com.bakdata.kafka.BlobStorageBackedStoringClient.CHARSET;
import static com.bakdata.kafka.BlobStorageBackedStoringClient.IS_BACKED;
import static com.bakdata.kafka.BlobStorageBackedStoringClient.IS_NOT_BACKED;

import java.net.URI;
import java.util.Objects;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Client for retrieving actual bytes of messages stored with {@link BlobStorageBackedStoringClient}.
 */
@Slf4j
@RequiredArgsConstructor
public class BlobStorageBackedRetrievingClient {

    private final @NonNull BlobStorageClient client;

    static BlobStorageURI deserializeUri(final byte[] data) {
        final byte[] uriBytes = getBytes(data);
        final String rawUri = new String(uriBytes, CHARSET);
        final URI uri = URI.create(rawUri);
        return new BlobStorageURI(uri);
    }

    static byte[] getBytes(final byte[] data) {
        final byte[] bytes = new byte[data.length - 1];
        // flag is stored in first byte
        System.arraycopy(data, 1, bytes, 0, data.length - 1);
        return bytes;
    }

    /**
     * Retrieve a payload that may have been stored on blob storage
     *
     * @param data payload
     * @return actual payload retrieved from blob storage
     */
    public byte[] retrieveBytes(final byte[] data) {
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
        Objects.requireNonNull(this.client);
        final BlobStorageURI uri = deserializeUri(data);
        final byte[] bytes = this.client.getObject(uri.getBucket(), uri.getKey());
        log.debug("Extracted large message from blob storage: {}", uri);
        return bytes;
    }
}
