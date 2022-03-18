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

import lombok.experimental.Delegate;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Kafka {@code Serde} that serializes large messages on blob storage.
 * <p>
 * It uses {@link LargeMessageSerializer} for serialization and {@link LargeMessageDeserializer} for deserialization.
 * <p>
 * For configuration options, see {@link LargeMessageSerdeConfig}.
 *
 * @param <T> type of records that can be (de-)serialized by this instance
 */
public class LargeMessageSerde<T> implements Serde<T> {
    @Delegate
    private final Serde<T> inner;

    /**
     * Default constructor
     */
    public LargeMessageSerde() {
        final Serializer<T> serializer = new LargeMessageSerializer<>();
        final Deserializer<T> deserializer = new LargeMessageDeserializer<>();
        this.inner = Serdes.serdeFrom(serializer, deserializer);
    }
}
