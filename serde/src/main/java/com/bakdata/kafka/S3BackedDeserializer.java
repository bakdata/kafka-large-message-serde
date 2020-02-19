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

import java.util.Map;
import java.util.Objects;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;

/**
 * Kafka {@code Deserializer} that deserializes messages serialized by {@link S3BackedSerializer}.
 * <p>
 * A message that is deserialized by this deserializer flags if it contains the actual message or if the actual message
 * is backed on Amazon S3. If the message is backed on S3, the actual message is downloaded. In any case, the
 * deserialization is delegated to a proper deserializer of this message type.
 * <p>
 * For configuration options, see {@link S3BackedSerdeConfig}.
 *
 * @param <T> type of records that can be deserialized by this instance
 */
@NoArgsConstructor
@Slf4j
public class S3BackedDeserializer<T> implements Deserializer<T> {
    private S3RetrievingClient client;
    private Deserializer<? extends T> deserializer;

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        final S3BackedSerdeConfig serdeConfig = new S3BackedSerdeConfig(configs);
        final Serde<T> serde = isKey ? serdeConfig.getKeySerde() : serdeConfig.getValueSerde();
        this.deserializer = serde.deserializer();
        this.client = serdeConfig.getS3Retriever();
        this.deserializer.configure(configs, isKey);
    }

    @Override
    public T deserialize(final String topic, final byte[] data) {
        Objects.requireNonNull(this.deserializer);
        Objects.requireNonNull(this.client);
        final byte[] bytes = this.client.retrieveBytes(data);
        return this.deserializer.deserialize(topic, bytes);
    }

    @Override
    public void close() {
        this.deserializer.close();
    }
}
