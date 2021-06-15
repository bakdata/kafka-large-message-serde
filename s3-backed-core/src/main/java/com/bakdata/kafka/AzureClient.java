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

@RequiredArgsConstructor
public class AzureClient implements BlobStorageClient {

    private final @NonNull BlobServiceClient blobServiceClient;

    private static String asURI(final String bucket, final String key) {
        return "abfs://" + bucket + "/" + key;
    }

    @Override
    public void deleteAllFiles(final String bucket, final String prefix) {
        final BlobContainerClient containerClient = this.blobServiceClient.createBlobContainer(bucket);
        final ListBlobsOptions options = new ListBlobsOptions().setPrefix(prefix);
        final PagedIterable<BlobItem> items = containerClient.listBlobs(options, null);
        items.forEach(blobItem -> containerClient.getBlobClient(blobItem.getName()).delete());
    }

    @Override
    public String put(final byte[] bytes, final String bucket, final String key) {
        final BlobContainerClient containerClient = this.blobServiceClient.createBlobContainer(bucket);
        final BlobClient blobClient = containerClient.getBlobClient(key);
        blobClient.upload(BinaryData.fromBytes(bytes));
        return asURI(bucket, key);
    }

    @Override
    public byte[] getObject(final String bucket, final String key) {
        final BlobContainerClient containerClient = this.blobServiceClient.createBlobContainer(bucket);
        final BlobClient blobClient = containerClient.getBlobClient(key);
        return blobClient.downloadContent().toBytes();
    }
}
