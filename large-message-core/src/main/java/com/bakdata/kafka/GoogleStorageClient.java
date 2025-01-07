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

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;


/**
 * Implementation of {@link BlobStorageClient} for Google Cloud Storage.
 */
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

    @Override
    public void close() {
        try {
            this.storage.close();
        } catch (final Exception e) {
            throw new RuntimeException("Error closing storage client", e);
        }
    }
}
