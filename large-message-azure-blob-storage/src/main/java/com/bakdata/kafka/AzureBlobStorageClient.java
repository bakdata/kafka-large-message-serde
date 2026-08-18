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

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Implementation of {@link BlobStorageClient} for Azure Blob Storage.
 */
@RequiredArgsConstructor
class AzureBlobStorageClient implements BlobStorageClient {

    static final String SCHEME = "abs";
    private final @NonNull BlobServiceClient blobServiceClient;

    private static String asURI(final String bucket, final String key) {
        return SCHEME + "://" + bucket + "/" + key;
    }

    @Override
    public void deleteAllObjects(final String bucket, final String prefix) {
        final BlobContainerClient containerClient = this.blobServiceClient.getBlobContainerClient(bucket);
        final ListBlobsOptions options = new ListBlobsOptions().setPrefix(prefix);
        final PagedIterable<BlobItem> items = containerClient.listBlobs(options, null);
        items.forEach(blobItem -> containerClient.getBlobClient(blobItem.getName()).delete());
    }

    @Override
    public String putObject(final byte[] bytes, final String bucket, final String key) {
        final BlobContainerClient containerClient = this.blobServiceClient.getBlobContainerClient(bucket);
        final BlobClient blobClient = containerClient.getBlobClient(key);
        blobClient.upload(BinaryData.fromBytes(bytes));
        return asURI(bucket, key);
    }

    @Override
    public byte[] getObject(final String bucket, final String key) {
        final BlobContainerClient containerClient = this.blobServiceClient.getBlobContainerClient(bucket);
        final BlobClient blobClient = containerClient.getBlobClient(key);
        return blobClient.downloadContent().toBytes();
    }

    @Override
    public void close() {
        // do nothing
    }
}
