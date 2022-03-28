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
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

/**
 * Kafka {@code Converter} that serializes large messages on blob storage.
 * <p>
 * For configuration options, see {@link LargeMessageConverterConfig}.
 */
public class LargeMessageConverter implements Converter {
    private Converter converter;
    private LargeMessageStoringClient storingClient;
    private LargeMessageRetrievingClient retrievingClient;
    private boolean isKey;

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        final LargeMessageConverterConfig config = new LargeMessageConverterConfig(configs);
        this.storingClient = config.getStorer();
        this.retrievingClient = config.getRetriever();
        this.isKey = isKey;
        this.converter = config.getConverter();
        this.converter.configure(configs, isKey);
    }

    /**
     * @since 2.2.0
     * @deprecated Use {@link #fromConnectData(String, Headers, Schema, Object)}
     */
    @Deprecated
    @Override
    public byte[] fromConnectData(final String topic, final Schema schema, final Object value) {
        return this.fromConnectData(topic, new RecordHeaders(), schema, value);
    }

    @Override
    public byte[] fromConnectData(final String topic, final Headers headers, final Schema schema, final Object value) {
        final byte[] inner = this.converter.fromConnectData(topic, schema, value);
        return this.storingClient.storeBytes(topic, inner, this.isKey, headers);
    }

    /**
     * @since 2.2.0
     * @deprecated Use {@link #toConnectData(String, Headers, byte[])}
     */
    @Deprecated
    @Override
    public SchemaAndValue toConnectData(final String topic, final byte[] value) {
        return this.toConnectData(topic, new RecordHeaders(), value);
    }

    @Override
    public SchemaAndValue toConnectData(final String topic, final Headers headers, final byte[] value) {
        final byte[] inner = this.retrievingClient.retrieveBytes(value, headers, this.isKey);
        return this.converter.toConnectData(topic, headers, inner);
    }

}
