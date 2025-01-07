/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
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

/**
 * Interface to access blob storage for getting, putting, and deleting blobs.
 */
public interface BlobStorageClient extends AutoCloseable {
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

    @Override
    void close();
}
