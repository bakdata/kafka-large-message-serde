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

import java.util.Map;
import java.util.Objects;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Kafka {@code Serializer} that serializes large messages on blob storage in order to cope with messages exceeding the
 * broker side maximum message size, usually indicated by
 * {@link org.apache.kafka.common.errors.RecordTooLargeException}.
 * <p>
 * Each message is serialized by a proper serializer for the message type. If the message size exceeds a defined
 * threshold, the payload is uploaded to blob storage. The message forwarded to Kafka contains a flag if the message has
 * been backed or not. In case it was backed, the flag is followed by the URI of the blob storage object. If the message
 * was not backed, it contains the actual serialized message.
 * <p>
 * For configuration options, see {@link LargeMessageSerdeConfig}.
 *
 * @param <T> type of records that can be serialized by this instance
 */
@NoArgsConstructor
@Slf4j
public class LargeMessageSerializer<T> implements Serializer<T> {
    private LargeMessageStoringClient client;
    private Serializer<? super T> serializer;
    private boolean isKey;

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        final LargeMessageSerdeConfig serdeConfig = new LargeMessageSerdeConfig(configs);
        final Serde<T> serde = isKey ? serdeConfig.getKeySerde() : serdeConfig.getValueSerde();
        this.serializer = serde.serializer();
        this.client = serdeConfig.getStorer();
        this.serializer.configure(configs, isKey);
        this.isKey = isKey;
    }

    /**
     * @since 2.2.0
     * @deprecated Use {@link #serialize(String, Headers, Object)}
     */
    @Deprecated
    @Override
    public byte[] serialize(final String topic, final T data) {
        Objects.requireNonNull(this.serializer);
        Objects.requireNonNull(this.client);
        final byte[] bytes = this.serializer.serialize(topic, data);
        return this.client.storeBytes(topic, bytes, this.isKey);
    }

    @Override
    public byte[] serialize(final String topic, final Headers headers, final T data) {
        Objects.requireNonNull(this.serializer);
        Objects.requireNonNull(this.client);
        final byte[] bytes = this.serializer.serialize(topic, headers, data);
        return this.client.storeBytes(topic, bytes, this.isKey, headers);
    }

    @Override
    public void close() {
        this.serializer.close();
    }
}
