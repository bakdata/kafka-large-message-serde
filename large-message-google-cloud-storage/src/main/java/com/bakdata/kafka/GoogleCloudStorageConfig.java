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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.StorageOptions;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

/**
 * This class provides default configuration options for Google Storage backed data. It offers configuration of the
 * following properties:
 * <ul>
 *     <li> Service account key JSON path
 * </ul>
 */
@Slf4j
@BlobStorageType(GoogleCloudStorageClient.SCHEME)
public class GoogleCloudStorageConfig extends AbstractConfig implements BlobStorageConfig {
    public static final String GOOGLE_STORAGE_PREFIX = PREFIX + GoogleCloudStorageClient.SCHEME + ".";
    public static final String GOOGLE_CLOUD_KEY_PATH = GOOGLE_STORAGE_PREFIX + "key.path";
    public static final String GOOGLE_CLOUD_KEY_PATH_DOC = "Path to the service account JSON file";
    public static final String GOOGLE_CLOUD_KEY_PATH_DEFAULT = "";
    private static final String GOOGLE_CLOUD_OAUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform";

    private static final ConfigDef config = baseConfigDef();

    /**
     * Create a new configuration from the given properties
     *
     * @param originals properties for configuring this config
     */
    public GoogleCloudStorageConfig(final Map<?, ?> originals) {
        super(config, originals);
    }

    protected static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(GOOGLE_CLOUD_KEY_PATH, Type.STRING, GOOGLE_CLOUD_KEY_PATH_DEFAULT, Importance.LOW,
                        GOOGLE_CLOUD_KEY_PATH_DOC)
                ;
    }

    /**
     * This method builds the Google Storage Client object. If you don't specify credentials when constructing the
     * client, the client library will look for credentials via the environment variable GOOGLE_APPLICATION_CREDENTIALS.
     * If the environment variable GOOGLE_APPLICATION_CREDENTIALS isn't set,  Application Default Credentials (ADC) uses
     * the service account that is attached to the resource that is running your code. For more information see the <a
     * href="https://cloud.google.com/docs/authentication/production#automatically">official documentation</a>
     *
     * @return GoogleStorageClient
     */
    @Override
    public BlobStorageClient createBlobStorageClient() {
        if (!this.getString(GOOGLE_CLOUD_KEY_PATH).isEmpty()) {
            final GoogleCredentials credentials = this.getGoogleCredentials();
            return new GoogleCloudStorageClient(
                    StorageOptions.newBuilder().setCredentials(credentials).build().getService());
        }
        return new GoogleCloudStorageClient(StorageOptions.getDefaultInstance().getService());
    }

    private GoogleCredentials getGoogleCredentials() {
        try (final FileInputStream credentialsStream = new FileInputStream(this.getString(GOOGLE_CLOUD_KEY_PATH))) {
            final List<String> scopes = List.of(GOOGLE_CLOUD_OAUTH_SCOPE);
            return GoogleCredentials.fromStream(credentialsStream).createScoped(scopes);
        } catch (final IOException ioException) {
            throw new UncheckedIOException(
                    "Please check if the JSON key file exists in the given path and try again.", ioException);
        }
    }
}
