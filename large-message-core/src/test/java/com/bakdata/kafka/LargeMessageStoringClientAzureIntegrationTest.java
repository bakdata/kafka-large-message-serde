/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata
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

import static com.bakdata.kafka.AzureBlobStorageClientIntegrationTest.getBlobServiceClient;
import static com.bakdata.kafka.AzureBlobStorageClientIntegrationTest.getBucketName;
import static com.bakdata.kafka.LargeMessageRetrievingClient.deserializeUri;
import static org.assertj.core.api.Assertions.assertThat;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Disabled("Requires Azure account")
class LargeMessageStoringClientAzureIntegrationTest {

    private static final String TOPIC = "output";
    private static final Deserializer<String> STRING_DESERIALIZER = Serdes.String().deserializer();
    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();

    private static Map<String, Object> createProperties(final Map<String, Object> properties) {
        return ImmutableMap.<String, Object>builder()
                .putAll(properties)
                .put(AbstractLargeMessageConfig.AZURE_CONNECTION_STRING_CONFIG,
                        System.getenv("AZURE_CONNECTION_STRING"))
                .build();
    }

    private static void expectBackedText(final String basePath, final String expected, final byte[] backedText,
            final String type) {
        final BlobStorageURI uri = deserializeUri(backedText);
        expectBackedText(basePath, expected, uri, type);
    }

    private static void expectBackedText(final String basePath, final String expected, final BlobStorageURI uri,
            final String type) {
        assertThat(uri).asString().startsWith(basePath + TOPIC + "/" + type + "/");
        final byte[] bytes = readBytes(uri);
        final String deserialized = STRING_DESERIALIZER.deserialize(null, bytes);
        assertThat(deserialized).isEqualTo(expected);
    }

    private static byte[] readBytes(final BlobStorageURI uri) {
        return getBlobServiceClient().getBlobContainerClient(uri.getBucket())
                .getBlobClient(uri.getKey())
                .downloadContent()
                .toBytes();
    }

    private static LargeMessageStoringClient createStorer(final Map<String, Object> baseProperties) {
        final Map<String, Object> properties = createProperties(baseProperties);
        final AbstractLargeMessageConfig config = new AbstractLargeMessageConfig(properties);
        return config.getStorer();
    }

    private static byte[] serialize(final String s) {
        return STRING_SERIALIZER.serialize(null, s);
    }

    @Test
    void shouldWriteBackedTextKey(final TestInfo testInfo) {
        final String bucket = getBucketName(testInfo);
        final String basePath = "abs://" + bucket + "/base/";
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, 0)
                .put(AbstractLargeMessageConfig.BASE_PATH_CONFIG, basePath)
                .build();
        final BlobServiceClient client = getBlobServiceClient();
        final BlobContainerClient containerClient = client.getBlobContainerClient(bucket);
        try {
            containerClient.create();
            final LargeMessageStoringClient storer = createStorer(properties);
            assertThat(storer.storeBytes(TOPIC, serialize("foo"), true))
                    .satisfies(backedText -> expectBackedText(basePath, "foo", backedText, "keys"));
        } finally {
            containerClient.delete();
        }
    }
}