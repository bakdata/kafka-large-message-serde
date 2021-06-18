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
import static com.bakdata.kafka.LargeMessageStoringClient.serialize;
import static org.assertj.core.api.Assertions.assertThat;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerClient;
import com.google.common.collect.ImmutableMap;
import io.confluent.common.config.ConfigDef;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Disabled("Requires Azure account")
class LargeMessageRetrievingClientAzureIntegrationTest {

    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();

    private static Map<String, Object> createProperties() {
        return ImmutableMap.<String, Object>builder()
                .put(AbstractLargeMessageConfig.AZURE_CONNECTION_STRING_CONFIG,
                        System.getenv("AZURE_CONNECTION_STRING"))
                .build();
    }

    private static void store(final String bucket, final String key, final String s) {
        getBlobServiceClient().getBlobContainerClient(bucket)
                .getBlobClient(key)
                .upload(BinaryData.fromBytes(s.getBytes()));
    }

    private static byte[] createBackedText(final String bucket, final String key) {
        final String uri = "abs://" + bucket + "/" + key;
        return serialize(uri);
    }

    private static LargeMessageRetrievingClient createRetriever() {
        final Map<String, Object> properties = createProperties();
        final ConfigDef configDef = AbstractLargeMessageConfig.baseConfigDef();
        final AbstractLargeMessageConfig config = new AbstractLargeMessageConfig(configDef, properties);
        return config.getRetriever();
    }

    @Test
    void shouldReadBackedText(final TestInfo testInfo) {
        final String bucket = getBucketName(testInfo);
        final BlobContainerClient containerClient = getBlobServiceClient().getBlobContainerClient(bucket);
        try {
            containerClient.create();
            final String key = "key";
            store(bucket, key, "foo");
            final LargeMessageRetrievingClient retriever = createRetriever();
            assertThat(retriever.retrieveBytes(createBackedText(bucket, key)))
                    .isEqualTo(STRING_SERIALIZER.serialize(null, "foo"));
        } finally {
            containerClient.delete();
        }
    }

}