package com.bakdata.kafka;

public interface BlobStorageConfig {
    BlobStorageClient createBlobStorageClient();
}
