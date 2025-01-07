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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

class GoogleCloudStorageTest {
    private static final String KEY = "key";
    private static final String BUCKET = "bucket";

    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();

    private final Storage storage = LocalStorageHelper.getOptions().getService();

    private static byte[] serialize(final String s) {
        return STRING_SERIALIZER.serialize(null, s);
    }

    @Test
    void shouldReadBackedText() {
        try (final BlobStorageClient googleStorageClient = new GoogleStorageClient(this.storage)) {
            googleStorageClient.putObject(serialize("foo"), BUCKET, KEY);
            assertThat(googleStorageClient.getObject(BUCKET, KEY)).isEqualTo(serialize("foo"));
        }
    }

    @Test
    void shouldWriteBackedText() {
        try (final BlobStorageClient googleStorageClient = new GoogleStorageClient(this.storage)) {
            final String expected = String.format("%s://%s/%s", GoogleStorageClient.SCHEME, BUCKET, KEY);
            assertThat(googleStorageClient.putObject(serialize("foo"), BUCKET, KEY)).isEqualTo(expected);
        }
    }

    @Test
    void shouldDeleteFiles() {
        try (final BlobStorageClient googleStorageClient = new GoogleStorageClient(this.storage)) {

            googleStorageClient.putObject(serialize("foo"), BUCKET, "base/foo/1");
            googleStorageClient.putObject(serialize("foo"), BUCKET, "base/foo/2");
            googleStorageClient.putObject(serialize("bar"), BUCKET, "base/bar/1");

            final Page<Blob> blobs = this.storage.list(BUCKET, Storage.BlobListOption.prefix("base/"));

            assertThat(blobs.iterateAll()).hasSize(3);
            googleStorageClient.deleteAllObjects(BUCKET, "base/foo/");
        }

        final Page<Blob> remainingBlob = this.storage.list(BUCKET, Storage.BlobListOption.prefix("base/"));
        assertThat(remainingBlob.iterateAll()).hasSize(1);
    }
}
