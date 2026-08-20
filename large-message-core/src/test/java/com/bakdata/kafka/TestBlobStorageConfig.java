/*
 * MIT License
 *
 * Copyright (c) 2026 bakdata
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

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

@BlobStorageType(TestBlobStorageConfig.SCHEME)
public class TestBlobStorageConfig extends AbstractConfig implements BlobStorageConfig {
    static final String SCHEME = "test";
    private final BlobStorageClient client = new TestBlobStorageClient();

    public TestBlobStorageConfig(final Map<?, ?> originals) {
        super(new ConfigDef(), originals);
    }

    @Override
    public BlobStorageClient createBlobStorageClient() {
        return this.client;
    }

    private static class TestBlobStorageClient implements BlobStorageClient {
        private final Map<String, byte[]> data = new HashMap<>();

        private static String asName(final String bucket, final String key) {
            return bucket + "/" + key;
        }

        @Override
        public void deleteAllObjects(final String bucket, final String prefix) {
            this.data.keySet().removeIf(name -> name.startsWith(asName(bucket, prefix)));
        }

        @Override
        public String putObject(final byte[] bytes, final String bucket, final String key) {
            final String name = asName(bucket, key);
            this.data.put(name, bytes);
            return SCHEME + "://" + name;
        }

        @Override
        public byte[] getObject(final String bucket, final String key) {
            final String name = asName(bucket, key);
            return this.data.get(name);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
