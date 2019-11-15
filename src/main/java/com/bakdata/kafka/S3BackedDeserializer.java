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

import static com.bakdata.kafka.S3BackedSerializer.CHARSET;
import static com.bakdata.kafka.S3BackedSerializer.IS_BACKED;
import static com.bakdata.kafka.S3BackedSerializer.IS_NOT_BACKED;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
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
    private AmazonS3 s3;
    private Deserializer<? extends T> deserializer;

    public static String deserializeUri(final byte[] data) {
        final byte[] uriBytes = getBytes(data);
        return new String(uriBytes, CHARSET);
    }

    public static byte[] getBytes(final byte[] data) {
        final byte[] bytes = new byte[data.length - 1];
        System.arraycopy(data, 1, bytes, 0, data.length - 1);
        return bytes;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        final S3BackedSerdeConfig serdeConfig = new S3BackedSerdeConfig(configs);
        final Serde<T> serde = isKey ? serdeConfig.getKeySerde() : serdeConfig.getValueSerde();
        this.deserializer = serde.deserializer();
        this.s3 = serdeConfig.getS3();
        this.deserializer.configure(configs, isKey);
    }

    @Override
    public T deserialize(final String topic, final byte[] data) {
        Objects.requireNonNull(this.deserializer);
        final byte[] bytes = this.extractBytes(data);
        return this.deserializer.deserialize(topic, bytes);
    }

    @Override
    public void close() {
        this.deserializer.close();
    }

    private byte[] extractBytes(final byte[] data) {
        if (data[0] == IS_NOT_BACKED) {
            return getBytes(data);
        }
        if (data[0] != IS_BACKED) {
            throw new IllegalArgumentException("Message can only be marked as backed or non-backed");
        }
        Objects.requireNonNull(this.s3);
        final String uri = deserializeUri(data);
        final AmazonS3URI s3URI = new AmazonS3URI(uri);
        try (final S3Object s3Object = this.s3.getObject(s3URI.getBucket(), s3URI.getKey());
                final InputStream in = s3Object.getObjectContent()) {
            final byte[] bytes = in.readAllBytes();
            log.info("Extracted large message from S3: {}", uri);
            return bytes;
        } catch (final IOException e) {
            throw new SerializationException("Cannot handle S3 backed message: " + s3URI, e);
        }
    }
}
