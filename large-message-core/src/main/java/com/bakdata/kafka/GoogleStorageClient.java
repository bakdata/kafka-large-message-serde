package com.bakdata.kafka;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class GoogleStorageClient implements BlobStorageClient {
    static final String SCHEME = "gs";
    private final @NonNull Storage storage;

    private static String asURI(final String bucket, final String key) {
        return SCHEME + "://" + bucket + "/" + key;
    }

    @Override
    public void deleteAllObjects(final String bucket, final String prefix) {
        final Page<Blob> blobs = this.storage.list(bucket, Storage.BlobListOption.currentDirectory(),
                Storage.BlobListOption.prefix(prefix));
        for (final Blob blob : blobs.iterateAll()) {
            this.storage.delete(blob.getBlobId());
        }
    }

    @Override
    public String putObject(final byte[] bytes, final String bucket, final String key) {
        final BlobId blobId = BlobId.of(bucket, key);
        final BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        this.storage.create(blobInfo, bytes);
        return asURI(bucket, key);
    }

    @Override
    public byte[] getObject(final String bucket, final String key) {
        final Blob blob = this.storage.get(BlobId.of(bucket, key));
        return blob.getContent();
    }
}
