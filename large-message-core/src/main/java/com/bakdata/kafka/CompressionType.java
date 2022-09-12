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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import lombok.Getter;

import lombok.NonNull;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import software.amazon.awssdk.utils.IoUtils;

/**
 * This enum specifies the various allowed compression types and their implementation.
 */
public enum CompressionType {
    NONE(0, "none") {
        @Override
        public byte[] compress(byte[] bytes) {
            return bytes;
        }

        @Override
        public byte[] decompress(byte[] bytes) {
            return bytes;
        }
    },

    GZIP(1, "gzip") {
        @Override
        public byte[] compress(byte[] bytes) {
            return CompressionType.compress(org.apache.kafka.common.record.CompressionType.GZIP, bytes);
        }

        @Override
        public byte[] decompress(byte[] bytes) {
            return CompressionType.decompress(org.apache.kafka.common.record.CompressionType.GZIP, bytes);
        }
    },

    SNAPPY(2, "snappy") {
        @Override
        public byte[] compress(byte[] bytes) {
            return CompressionType.compress(org.apache.kafka.common.record.CompressionType.SNAPPY, bytes);
        }

        @Override
        public byte[] decompress(byte[] bytes) {
            return CompressionType.decompress(org.apache.kafka.common.record.CompressionType.SNAPPY, bytes);
        }
    },

    LZ4(3, "lz4") {
        @Override
        public byte[] compress(byte[] bytes) {
            return CompressionType.compress(org.apache.kafka.common.record.CompressionType.LZ4, bytes);
        }

        @Override
        public byte[] decompress(byte[] bytes) {
            return CompressionType.decompress(org.apache.kafka.common.record.CompressionType.LZ4, bytes);
        }
    },

    ZSTD(4, "zstd") {
        @Override
        public byte[] compress(byte[] bytes) {
            return CompressionType.compress(org.apache.kafka.common.record.CompressionType.ZSTD, bytes);
        }

        @Override
        public byte[] decompress(byte[] bytes) {
            return CompressionType.decompress(org.apache.kafka.common.record.CompressionType.ZSTD, bytes);
        }
    };

    private static byte[] compress(org.apache.kafka.common.record.CompressionType compressionType, byte[] bytes) {
        final ByteBufferOutputStream outStream = new ByteBufferOutputStream(bytes.length);
        try (OutputStream stream = compressionType.wrapForOutput(outStream, RecordBatch.MAGIC_VALUE_V2)) {
            stream.write(bytes);
            stream.flush();
        } catch (IOException e) {
            throw new SerializationException("Failed to compress with type " + compressionType, e);
        }

        return outStream.buffer().array();
    }

    private static final BufferSupplier bufferSupplier = BufferSupplier.create();

    @Getter
    private final byte id;
    @NonNull
    @Getter
    private final String name;

    CompressionType(int id, String name) {
        this.id = (byte)id;
        this.name = name;
    }

    public static final String HEADER_NAME = HeaderLargeMessagePayloadProtocol.HEADER_PREFIX + ".compression";

    private static byte[] decompress(org.apache.kafka.common.record.CompressionType compressionType, byte[] bytes) {
        try (InputStream stream = compressionType.wrapForInput(ByteBuffer.wrap(bytes), RecordBatch.MAGIC_VALUE_V2, bufferSupplier)) {
            return IoUtils.toByteArray(stream);
        } catch (IOException e) {
            throw new SerializationException("Failed to compress with type " + compressionType, e);
        }
    }

    public static CompressionType forId(int id) {
        switch (id) {
            case 0:
                return NONE;
            case 1:
                return GZIP;
            case 2:
                return SNAPPY;
            case 3:
                return LZ4;
            case 4:
                return ZSTD;
            default:
                throw new IllegalArgumentException("Unknown compression type id: " + id);
        }
    }

    public static CompressionType forName(String name) {
        if (NONE.name.equals(name))
            return NONE;
        else if (GZIP.name.equals(name))
            return GZIP;
        else if (SNAPPY.name.equals(name))
            return SNAPPY;
        else if (LZ4.name.equals(name))
            return LZ4;
        else if (ZSTD.name.equals(name))
            return ZSTD;
        else
            throw new IllegalArgumentException("Unknown compression name: " + name);
    }
    
    public abstract byte[] compress(byte[] bytes);
    public abstract byte[] decompress(byte[] bytes);
}
