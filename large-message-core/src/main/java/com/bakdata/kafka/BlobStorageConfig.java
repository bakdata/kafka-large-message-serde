package com.bakdata.kafka;

@FunctionalInterface
public interface BlobStorageConfig {
    BlobStorageClient createBlobStorageClient();
}
