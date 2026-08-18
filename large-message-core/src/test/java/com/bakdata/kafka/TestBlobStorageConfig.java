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

        }
    }
}
