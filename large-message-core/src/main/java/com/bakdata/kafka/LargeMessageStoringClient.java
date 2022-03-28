/*
 * MIT License
 *
 * Copyright (c) 2022 bakdata
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

import java.util.Objects;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;

/**
 * Client for storing large {@code byte[]} on blob storage if the size exceeds a defined limit.
 */
@Slf4j
@Builder
public class LargeMessageStoringClient {

    private static final String VALUE_PREFIX = "values";
    private static final String KEY_PREFIX = "keys";
    private final @NonNull BlobStorageClient client;
    private final BlobStorageURI basePath;
    private final int maxSize;
    private final IdGenerator idGenerator;
    private final @NonNull LargeMessagePayloadProtocol protocol;
    private final boolean isKey;

    private static byte[] serialize(final LargeMessagePayloadProtocol protocol, final String uri,
            final Headers headers) {
        return protocol.serialize(LargeMessagePayload.ofUri(uri), headers);
    }

    private static byte[] serialize(final LargeMessagePayloadProtocol protocol, final byte[] bytes,
            final Headers headers) {
        return protocol.serialize(LargeMessagePayload.ofBytes(bytes), headers);
    }

    private static String toString(final String s) {
        return s == null ? "" : s;
    }

    /**
     * Store bytes on blob storage if they exceed the configured maximum size.
     *
     * @param topic name of the topic the bytes are associated with
     * @param bytes payload
     * @param headers headers used to store flag distinguishing blob storage backed payloads
     * @return bytes representing the payload. Can be read using {@link LargeMessageRetrievingClient}
     */
    public byte[] storeBytes(final String topic, final byte[] bytes, final Headers headers) {
        if (bytes == null) {
            return null;
        }
        if (this.needsBacking(bytes)) {
            final String key = this.createBlobStorageKey(topic, bytes);
            final String uri = this.uploadToBlobStorage(key, bytes);
            return serialize(this.protocol, uri, headers);
        } else {
            return serialize(this.protocol, bytes, headers);
        }
    }

    /**
     * Delete all files associated with a topic from blob storage
     *
     * @param topic name of the topic
     */
    public void deleteAllFiles(final String topic) {
        Objects.requireNonNull(this.basePath, "Base path must not be null");
        final String prefix = this.createTopicPrefix(topic);
        final String bucketName = this.basePath.getBucket();
        log.info("Deleting blob storage backed files for topic '{}'", topic);
        this.client.deleteAllObjects(bucketName, prefix);
        log.info("Finished deleting blob storage backed files for topic '{}'", topic);
    }

    private String createBlobStorageKey(final String topic, final byte[] bytes) {
        Objects.requireNonNull(this.basePath, "Base path must not be null");
        Objects.requireNonNull(this.idGenerator, "Id generator must not be null");
        final String id = this.idGenerator.generateId(bytes);
        return this.createTopicPrefix(topic) + id;
    }

    private String createTopicPrefix(final String topic) {
        Objects.requireNonNull(topic, "Topic must not be null");
        final String prefix = this.isKey ? KEY_PREFIX : VALUE_PREFIX;
        return toString(this.basePath.getKey()) + topic + "/" + prefix + "/";
    }

    private boolean needsBacking(final byte[] bytes) {
        return bytes.length >= this.maxSize;
    }

    private String uploadToBlobStorage(final String key, final byte[] bytes) {
        final String bucket = this.basePath.getBucket();
        final String uri = this.client.putObject(bytes, bucket, key);
        log.debug("Stored large message on blob storage: {}", uri);
        return uri;
    }
}
