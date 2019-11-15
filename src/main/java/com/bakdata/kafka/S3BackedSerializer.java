/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata
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

import com.amazonaws.services.s3.AmazonS3URI;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Kafka {@code Serializer} that serializes large messages on Amazon S3 in order to cope with messages exceeding the
 * broker side maximum message size, usually indicated by
 * {@link org.apache.kafka.common.errors.RecordTooLargeException}.
 * <p>
 * Each message is serialized by a proper serializer for the message type. If the message size exceeds a defined
 * threshold, the payload is uploaded to Amazon S3. The message forwarded to Kafka contains a flag if the message has
 * been backed or not. In case it was backed, the flag is followed by the URI of the S3 object. If the message was not
 * backed, it contains the actual serialized message.
 * <p>
 * For configuration options, see {@link S3BackedSerdeConfig}.
 *
 * @param <T> type of records that can be serialized by this instance
 */
@NoArgsConstructor
@Slf4j
public class S3BackedSerializer<T> implements Serializer<T> {
    private static final String VALUE_PREFIX = "values";
    private static final String KEY_PREFIX = "keys";
    private S3BackingClient s3;
    private Serializer<? super T> serializer;
    private AmazonS3URI basePath;
    private int maxSize;
    private String prefix;

    private static String toString(final String s) {
        return s == null ? "" : s;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        final S3BackedSerdeConfig serdeConfig = new S3BackedSerdeConfig(configs);
        final Serde<T> serde = isKey ? serdeConfig.getKeySerde() : serdeConfig.getValueSerde();
        this.serializer = serde.serializer();
        this.maxSize = serdeConfig.getMaxSize();
        this.basePath = serdeConfig.getBasePath();
        this.s3 = serdeConfig.getS3();
        this.serializer.configure(configs, isKey);
        this.prefix = isKey ? KEY_PREFIX : VALUE_PREFIX;
    }

    @Override
    public byte[] serialize(final String topic, final T data) {
        Objects.requireNonNull(this.serializer);
        final byte[] bytes = this.serializer.serialize(topic, data);
        if (this.needsS3Backing(bytes)) {
            Objects.requireNonNull(this.basePath);
            Objects.requireNonNull(this.s3);
            final String bucket = this.basePath.getBucket();
            final String key = this.createS3Key(topic);
            final String uri = this.s3.uploadToS3(bucket, key, bytes);
            return S3BackingClient.serialize(uri);
        } else {
            return S3BackingClient.serialize(bytes);
        }
    }

    @Override
    public void close() {
        this.serializer.close();
    }

    private String createS3Key(final @NonNull String topic) {
        return toString(this.basePath.getKey()) + topic + "/" + this.prefix + "/" + UUID.randomUUID();
    }

    private boolean needsS3Backing(final byte[] bytes) {
        return bytes.length >= this.maxSize;
    }
}
