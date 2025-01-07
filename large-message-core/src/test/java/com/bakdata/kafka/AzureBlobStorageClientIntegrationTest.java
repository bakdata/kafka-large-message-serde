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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

class AzureBlobStorageClientIntegrationTest extends AzureBlobStorageIntegrationTest {

    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();

    private static void store(final BlobContainerClient bucket, final String key, final String s) {
        bucket.getBlobClient(key)
                .upload(BinaryData.fromBytes(s.getBytes()));
    }

    private static byte[] serialize(final String s) {
        return STRING_SERIALIZER.serialize(null, s);
    }

    private static ListBlobsOptions withPrefix(final String prefix) {
        return new ListBlobsOptions().setPrefix(prefix);
    }

    @Test
    void shouldReadBackedText() {
        final String bucket = "bucket";
        final BlobServiceClient blobServiceClient = this.getBlobServiceClient();
        final BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(bucket);
        try {
            containerClient.create();
            final String key = "key";
            store(containerClient, key, "foo");
            try (final BlobStorageClient client = new AzureBlobStorageClient(blobServiceClient)) {
                assertThat(client.getObject(bucket, key))
                        .isEqualTo(serialize("foo"));
            }
        } finally {
            containerClient.delete();
        }
    }

    @Test
    void shouldWriteBackedText() {
        final String bucket = "bucket";
        final String key = "key";
        final BlobServiceClient blobServiceClient = this.getBlobServiceClient();
        final BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(bucket);
        try {
            containerClient.create();
            try (final BlobStorageClient client = new AzureBlobStorageClient(blobServiceClient)) {
                assertThat(client.putObject(serialize("foo"), bucket, key))
                        .isEqualTo("abs://" + bucket + "/key");
            }
        } finally {
            containerClient.delete();
        }
    }

    @Test
    void shouldDeleteFiles() {
        final String bucket = "bucket";
        final BlobServiceClient blobServiceClient = this.getBlobServiceClient();
        final BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(bucket);
        containerClient.create();
        try {
            try (final BlobStorageClient client = new AzureBlobStorageClient(blobServiceClient)) {
                client.putObject(serialize("foo"), bucket, "base/foo/1");
                client.putObject(serialize("foo"), bucket, "base/foo/2");
                client.putObject(serialize("foo"), bucket, "base/bar/1");
                assertThat(containerClient.listBlobs(withPrefix("base/"), null).stream()
                        .collect(Collectors.toList())).hasSize(3);
                client.deleteAllObjects(bucket, "base/foo/");
            }
            assertThat(containerClient.listBlobs(withPrefix("base/"), null).stream()
                    .collect(Collectors.toList())).hasSize(1);
        } finally {
            containerClient.delete();
        }
    }

    @Test
    void shouldThrowExceptionOnMissingObject() {
        final String bucket = "bucket";
        final BlobServiceClient blobServiceClient = this.getBlobServiceClient();
        final BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(bucket);
        try {
            containerClient.create();
            final String key = "key";
            try (final BlobStorageClient client = new AzureBlobStorageClient(blobServiceClient)) {
                assertThatExceptionOfType(BlobStorageException.class)
                        .isThrownBy(() -> client.getObject(bucket, key))
                        .withMessageContaining("The specified blob does not exist.");
            }
        } finally {
            containerClient.delete();
        }
    }

    @Test
    void shouldThrowExceptionOnMissingBucketForGet() {
        final String bucket = "bucket";
        final String key = "key";
        try (final BlobStorageClient client = new AzureBlobStorageClient(this.getBlobServiceClient())) {
            assertThatExceptionOfType(BlobStorageException.class)
                    .isThrownBy(() -> client.getObject(bucket, key))
                    .withMessageContaining("The specified container does not exist.");
        }
    }

    @Test
    void shouldThrowExceptionOnMissingBucketForPut() {
        final String bucket = "bucket";
        final String key = "key";
        try (final BlobStorageClient client = new AzureBlobStorageClient(this.getBlobServiceClient())) {
            final byte[] foo = serialize("foo");
            assertThatExceptionOfType(BlobStorageException.class)
                    .isThrownBy(() -> client.putObject(foo, bucket, key))
                    .withMessageContaining("The specified container does not exist.");
        }
    }

}
