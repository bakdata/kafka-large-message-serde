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

import static com.bakdata.kafka.HeaderLargeMessagePayloadSerde.usesHeaders;
import static com.bakdata.kafka.LargeMessageStoringClient.CHARSET;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;

/**
 * Client for retrieving actual bytes of messages stored with {@link LargeMessageStoringClient}.
 */
@Slf4j
@RequiredArgsConstructor
public class LargeMessageRetrievingClient {

    private final @NonNull Map<String, Supplier<BlobStorageClient>> clientFactories;
    private final @NonNull Map<String, BlobStorageClient> clientCache = new HashMap<>();

    static BlobStorageURI deserializeUri(final byte[] uriBytes) {
        final String rawUri = new String(uriBytes, CHARSET);
        return BlobStorageURI.create(rawUri);
    }

    private static LargeMessagePayloadSerde getSerde(final Headers headers) {
        if (usesHeaders(headers)) {
            return new HeaderLargeMessagePayloadSerde(headers);
        } else {
            return ByteArrayLargeMessagePayloadSerde.INSTANCE;
        }
    }

    /**
     * Retrieve a payload that may have been stored on blob storage
     *
     * @param data payload
     * @param headers headers that might contain flag to distinguish blob storage backed messages
     * @return actual payload retrieved from blob storage
     */
    public byte[] retrieveBytes(final byte[] data, final Headers headers) {
        if (data == null) {
            return null;
        }
        final LargeMessagePayloadSerde serde = getSerde(headers);
        final LargeMessagePayload payload = serde.deserialize(data);
        final byte[] deserializedData = payload.getData();
        if (payload.isBacked()) {
            return this.retrieveBackedBytes(deserializedData);
        } else {
            return deserializedData;
        }
    }

    private byte[] retrieveBackedBytes(final byte[] data) {
        final BlobStorageURI uri = deserializeUri(data);
        final BlobStorageClient client = this.getClient(uri);
        Objects.requireNonNull(client);
        final byte[] bytes = client.getObject(uri.getBucket(), uri.getKey());
        log.debug("Extracted large message from blob storage: {}", uri);
        return bytes;
    }

    private BlobStorageClient getClient(final BlobStorageURI uri) {
        final String scheme = uri.getScheme();
        return this.clientCache.computeIfAbsent(scheme, this::createClient);
    }

    private BlobStorageClient createClient(final String scheme) {
        return Optional.ofNullable(this.clientFactories.get(scheme))
                .map(Supplier::get)
                .orElseThrow(() -> AbstractLargeMessageConfig.unknownScheme(scheme));
    }
}
