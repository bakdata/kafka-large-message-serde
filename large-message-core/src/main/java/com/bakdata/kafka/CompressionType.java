package com.bakdata.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import software.amazon.awssdk.utils.IoUtils;

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
        ByteBufferOutputStream outStream = new ByteBufferOutputStream(bytes.length);
        try (OutputStream stream = compressionType.wrapForOutput(outStream, RecordBatch.MAGIC_VALUE_V2)) {
            stream.write(bytes);
            stream.flush();
        } catch (IOException e) {
            throw new SerializationException("Failed to compress with type " + compressionType, e);
        }

        return outStream.buffer().array();
    }

    private static final BufferSupplier bufferSupplier = BufferSupplier.create();

    private static byte[] decompress(org.apache.kafka.common.record.CompressionType compressionType, byte[] bytes) {
        try (InputStream stream = compressionType.wrapForInput(ByteBuffer.wrap(bytes), RecordBatch.MAGIC_VALUE_V2, bufferSupplier)) {
            return IoUtils.toByteArray(stream);
        } catch (IOException e) {
            throw new SerializationException("Failed to compress with type " + compressionType, e);
        }
    }

    public final byte id;
    public final String name;

    CompressionType(int id, String name) {
        this.id = (byte)id;
        this.name = name;
    }

    public static final String HEADER_NAME = HeaderLargeMessagePayloadProtocol.HEADER_PREFIX + ".compression";

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
