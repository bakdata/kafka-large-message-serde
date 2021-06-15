package com.bakdata.kafka;

public interface BlobStorageClient {
    void deleteAllFiles(String bucket, String prefix);

    String put(byte[] bytes, String bucket, String key);

    byte[] getObject(String bucket, String key);
}
