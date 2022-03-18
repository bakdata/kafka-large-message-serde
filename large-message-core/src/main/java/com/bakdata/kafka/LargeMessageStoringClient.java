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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Client for storing large {@code byte[]} on blob storage if the size exceeds a defined limit.
 */
@Slf4j
@Builder
public class LargeMessageStoringClient {

    static final byte IS_NOT_BACKED = 0;
    static final byte IS_BACKED = 1;
    static final Charset CHARSET = StandardCharsets.UTF_8;
    private static final String VALUE_PREFIX = "values";
    private static final String KEY_PREFIX = "keys";
    private final @NonNull BlobStorageClient client;
    private final BlobStorageURI basePath;
    private final int maxSize;
    private final IdGenerator idGenerator;

    static byte[] serialize(final String uri) {
        final byte[] uriBytes = uri.getBytes(CHARSET);
        return serialize(uriBytes, IS_BACKED);
    }

    static byte[] serialize(final byte[] bytes) {
        return serialize(bytes, IS_NOT_BACKED);
    }

    private static String toString(final String s) {
        return s == null ? "" : s;
    }

    private static byte[] serialize(final byte[] bytes, final byte flag) {
        final byte[] fullBytes = new byte[bytes.length + 1];
        fullBytes[0] = flag;
        System.arraycopy(bytes, 0, fullBytes, 1, bytes.length);
        return fullBytes;
    }

    /**
     * Store bytes on blob storage if they exceed the configured maximum size.
     *
     * @param topic name of the topic the bytes are associated with
     * @param bytes payload
     * @param isKey whether the bytes represent the key of a message
     * @return bytes representing the payload. Can be read using {@link LargeMessageRetrievingClient}
     */
    public byte[] storeBytes(final String topic, final byte[] bytes, final boolean isKey) {
        if (bytes == null) {
            return null;
        }
        if (this.needsBacking(bytes)) {
            final String key = this.createBlobStorageKey(topic, isKey, bytes);
            final String uri = this.uploadToBlobStorage(key, bytes);
            return serialize(uri);
        } else {
            return serialize(bytes);
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

    private String createBlobStorageKey(final String topic, final boolean isKey, final byte[] bytes) {
        Objects.requireNonNull(this.basePath, "Base path must not be null");
        Objects.requireNonNull(this.idGenerator, "Id generator must not be null");
        final String prefix = isKey ? KEY_PREFIX : VALUE_PREFIX;
        final String id = this.idGenerator.generateId(bytes);
        return this.createTopicPrefix(topic) + prefix + "/" + id;
    }

    private String createTopicPrefix(final String topic) {
        Objects.requireNonNull(topic, "Topic must not be null");
        return toString(this.basePath.getKey()) + topic + "/";
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
