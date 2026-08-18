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

import static com.bakdata.kafka.AbstractLargeMessageConfig.PREFIX;
import static com.bakdata.kafka.AbstractLargeMessageConfig.isEmpty;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

/**
 * This class provides default configuration options for Azure Blob Storage backed data. It offers configuration of the
 * following properties:
 * <ul>
 *     <li> Connection string
 * </ul>
 */
@Slf4j
@BlobStorageType(AzureBlobStorageClient.SCHEME)
public class AzureBlobStorageConfig extends AbstractConfig implements BlobStorageConfig {
    public static final String AZURE_PREFIX = PREFIX + AzureBlobStorageClient.SCHEME + ".";
    public static final String AZURE_CONNECTION_STRING_CONFIG = AZURE_PREFIX + "connection.string";
    public static final String AZURE_CONNECTION_STRING_DOC = "Azure connection string for connection to blob storage. "
            + "Leave empty if Azure credential provider chain should be used.";
    public static final String AZURE_CONNECTION_STRING_DEFAULT = "";

    private static final ConfigDef config = baseConfigDef();

    /**
     * Create a new configuration from the given properties
     *
     * @param originals properties for configuring this config
     */
    public AzureBlobStorageConfig(final Map<?, ?> originals) {
        super(config, originals);
    }

    protected AzureBlobStorageConfig(final ConfigDef config, final Map<?, ?> originals) {
        super(config, originals);
    }

    protected static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(AZURE_CONNECTION_STRING_CONFIG, Type.PASSWORD, AZURE_CONNECTION_STRING_DEFAULT, Importance.LOW,
                        AZURE_CONNECTION_STRING_DOC)
                ;
    }

    @Override
    public BlobStorageClient createBlobStorageClient() {
        final BlobServiceClientBuilder clientBuilder = new BlobServiceClientBuilder();
        this.getAzureConnectionString().ifPresent(clientBuilder::connectionString);
        final BlobServiceClient blobServiceClient = clientBuilder.buildClient();
        return new AzureBlobStorageClient(blobServiceClient);
    }

    private Optional<String> getAzureConnectionString() {
        final String connectionString = this.getPassword(AZURE_CONNECTION_STRING_CONFIG).value();
        return isEmpty(connectionString) ? Optional.empty() : Optional.of(connectionString);
    }
}
