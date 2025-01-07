/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
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

import static com.bakdata.kafka.HeaderLargeMessagePayloadProtocol.getHeaderName;

import java.util.Map;
import java.util.Objects;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;

/**
 * Kafka {@code Deserializer} that deserializes messages serialized by {@link LargeMessageSerializer}.
 * <p>
 * A message that is deserialized by this deserializer flags if it contains the actual message or if the actual message
 * is backed on blob storage. If the message is backed on blob storage, the actual message is downloaded. In any case,
 * the deserialization is delegated to a proper deserializer of this message type.
 * <p>
 * For configuration options, see {@link LargeMessageSerdeConfig}.
 *
 * @param <T> type of records that can be deserialized by this instance
 */
@NoArgsConstructor
@Slf4j
public class LargeMessageDeserializer<T> implements Deserializer<T> {
    private LargeMessageRetrievingClient client;
    private Deserializer<? extends T> deserializer;
    private boolean isKey;

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        final LargeMessageSerdeConfig serdeConfig = new LargeMessageSerdeConfig(configs);
        final Serde<T> serde = isKey ? serdeConfig.getKeySerde() : serdeConfig.getValueSerde();
        this.deserializer = serde.deserializer();
        this.client = serdeConfig.getRetriever();
        this.deserializer.configure(configs, isKey);
        this.isKey = isKey;
    }


    @Override
    public T deserialize(final String topic, final byte[] data) {
        Objects.requireNonNull(this.deserializer);
        Objects.requireNonNull(this.client);
        final byte[] bytes = this.client.retrieveBytes(data, this.isKey);
        return this.deserializer.deserialize(topic, bytes);
    }

    @Override
    public T deserialize(final String topic, final Headers headers, final byte[] data) {
        Objects.requireNonNull(this.deserializer);
        Objects.requireNonNull(this.client);
        final byte[] bytes = this.client.retrieveBytes(data, headers, this.isKey);
        final T deserialized = this.deserializer.deserialize(topic, headers, bytes);
        // remove all headers associated with large message because the record might be serialized with different flags
        headers.remove(getHeaderName(this.isKey));
        headers.remove(CompressionType.HEADER_NAME);
        return deserialized;
    }

    @Override
    public void close() {
        this.deserializer.close();
        this.client.close();
    }
}
