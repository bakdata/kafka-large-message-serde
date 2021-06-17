package com.bakdata.kafka;

/**
 * Interface to access blob storage for getting, putting, and deleting blobs.
 */
public interface BlobStorageClient {
    /**
     * Delete all objects in a bucket associated with a specified prefix
     *
     * @param bucket the bucket to delete from
     * @param prefix the prefix for which blobs should be deleted
     */
    void deleteAllObjects(String bucket, String prefix);

    /**
     * Store a payload in a bucket
     *
     * @param bytes the payload
     * @param bucket the bucket where the payload should be stored
     * @param key the identifier for the payload within the bucket
     * @return unique identifier to retrieve the payload
     */
    String putObject(byte[] bytes, String bucket, String key);

    /**
     * Retrieve a payload from a bucket
     *
     * @param bucket the bucket where the payload is stored
     * @param key the identifier for the payload within the bucket
     * @return the payload
     */
    byte[] getObject(String bucket, String key);
}
