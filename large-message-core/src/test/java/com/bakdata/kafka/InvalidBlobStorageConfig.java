package com.bakdata.kafka;

import static java.util.Collections.emptyMap;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

@BlobStorageType(InvalidBlobStorageConfig.SCHEME)
public class InvalidBlobStorageConfig extends AbstractConfig implements BlobStorageConfig {
    static final String SCHEME = "invalid";

    public InvalidBlobStorageConfig() {
        super(new ConfigDef(), emptyMap());
    }

    @Override
    public BlobStorageClient createBlobStorageClient() {
        throw new UnsupportedOperationException();
    }
}
