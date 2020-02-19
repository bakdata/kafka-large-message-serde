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

import java.util.Map;
import lombok.NoArgsConstructor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

@NoArgsConstructor
public class S3BackedConverter implements Converter {
    private Converter converter;
    private S3StoringClient s3StoringClient;
    private S3RetrievingClient s3RetrievingClient;
    private boolean isKey;

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        final S3BackedConverterConfig config = new S3BackedConverterConfig(configs);
        this.s3StoringClient = config.getS3Storer();
        this.s3RetrievingClient = config.getS3Retriever();
        this.isKey = isKey;
        this.converter = config.getConverter();
        this.converter.configure(configs, isKey);
    }

    @Override
    public byte[] fromConnectData(final String topic, final Schema schema, final Object value) {
        final byte[] inner = this.converter.fromConnectData(topic, schema, value);
        return this.s3StoringClient.storeBytes(topic, inner, this.isKey);
    }

    @Override
    public SchemaAndValue toConnectData(final String topic, final byte[] value) {
        final byte[] inner = this.s3RetrievingClient.retrieveBytes(value);
        return this.converter.toConnectData(topic, inner);
    }

}
