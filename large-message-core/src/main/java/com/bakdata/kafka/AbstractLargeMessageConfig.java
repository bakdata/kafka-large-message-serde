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

import static software.amazon.awssdk.utils.StringUtils.isEmpty;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.utils.Utils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

/**
 * This class provides default configuration options for blob storage backed data. It offers configuration of the
 * following properties:
 * <p></p>
 * General
 * <ul>
 *     <li> maximum serialized message size in bytes
 *     <li> base path
 *     <li> id generator
 *     <li> usage of headers to store large message flag
 *     <li> acceptance of no headers as signal that message is not backed
 *     <li> compression type
 * </ul>
 * <p></p>
 * Amazon S3 specific
 * <ul>
 *     <li> S3 endpoint
 *     <li> S3 region
 *     <li> S3 access key
 *     <li> S3 secret key
 *     <li> AWS security token service
 *     <li> AWS OIDC token path
 * </ul>
 * <p></p>
 * Azure Blob Storage specific
 * <ul>
 *     <li> Connection string
 * </ul>
 * <p></p>
 * Google Cloud Storage specific
 * <ul>
 *     <li> Service account key JSON path
 * </ul>
 */
@Slf4j
public class AbstractLargeMessageConfig extends AbstractConfig {
    public static final String PREFIX = "large.message.";
    public static final String MAX_BYTE_SIZE_CONFIG = PREFIX + "max.byte.size";
    public static final String MAX_BYTE_SIZE_DOC =
            "Maximum serialized message size in bytes before messages are stored on blob storage.";
    public static final int MAX_BYTE_SIZE_DEFAULT = 1000 * 1000;
    public static final String BASE_PATH_CONFIG = PREFIX + "base.path";
    public static final String BASE_PATH_DOC = "Base path to store data. Must include bucket and any prefix that "
            + "should be used, e.g., 's3://my-bucket/my/prefix/'. Available protocols: 's3', 'abs'.";
    public static final String BASE_PATH_DEFAULT = "";
    public static final String ID_GENERATOR_CONFIG = PREFIX + "id.generator";
    public static final String ID_GENERATOR_DOC = "Class to use for generating unique object IDs. Available "
            + "generators are: " + RandomUUIDGenerator.class.getName() + ", " + Sha256HashIdGenerator.class.getName()
            + ", " + MurmurHashIdGenerator.class.getName() + ".";
    public static final Class<? extends IdGenerator> ID_GENERATOR_DEFAULT = RandomUUIDGenerator.class;
    public static final String USE_HEADERS_CONFIG = PREFIX + "use.headers";
    public static final String USE_HEADERS_DOC =
            "Enable if Kafka message headers should be used to distinguish blob storage backed messages. This is "
                    + "disabled by default for backwards compatibility but leads to increased memory usage. It is "
                    + "recommended to enable this option.";
    public static final boolean USE_HEADERS_DEFAULT = false;
    public static final String ACCEPT_NO_HEADERS_CONFIG = PREFIX + "accept.no.headers";
    public static final String ACCEPT_NO_HEADERS_DOC =
            "Enable if messages read with no headers should be treated as non-backed messages. This allows enabling "
                    + "of large message behavior for data that has been serialized using the wrapped serializer.";
    public static final boolean ACCEPT_NO_HEADERS_DEFAULT = false;

    public static final String COMPRESSION_TYPE_CONFIG = PREFIX + "compression.type";
    public static final String COMPRESSION_TYPE_DOC =
            "The compression type for data stored in blob storage. The default is none (i.e. no compression). Valid "
                    + " values are <code>none</code>, <code>gzip</code>, <code>snappy</code>, <code>lz4</code>, or "
                    + "<code>zstd</code>. Note: this option is only available when kafka message headers are used.";
    public static final String COMPRESSION_TYPE_DEFAULT = "none";

    public static final String S3_PREFIX = PREFIX + AmazonS3Client.SCHEME + ".";
    public static final String S3_ENDPOINT_CONFIG = S3_PREFIX + "endpoint";
    public static final String S3_REGION_CONFIG = S3_PREFIX + "region";
    public static final String S3_ACCESS_KEY_CONFIG = S3_PREFIX + "access.key";
    public static final String S3_SECRET_KEY_CONFIG = S3_PREFIX + "secret.key";
    public static final String S3_ROLE_EXTERNAL_ID_CONFIG = S3_PREFIX + "sts.role.external.id";
    public static final String S3_ROLE_ARN_CONFIG = S3_PREFIX + "sts.role.arn";
    public static final String S3_ROLE_SESSION_NAME_CONFIG = S3_PREFIX + "sts.role.session.name";
    public static final String S3_JWT_PATH_CONFIG = S3_PREFIX + "jwt.path";
    public static final String S3_REGION_DOC = "S3 region to use. Leave empty if default S3 region should be used.";
    public static final String S3_ENDPOINT_DOC =
            "Endpoint to use for connection to Amazon S3. Leave empty if default S3 endpoint should be used.";
    public static final String S3_ENDPOINT_DEFAULT = "";
    public static final String S3_ENABLE_PATH_STYLE_ACCESS_CONFIG = S3_PREFIX + "path.style.access";
    public static final String S3_ENABLE_PATH_STYLE_ACCESS_DOC = "Enable path-style access for S3 client.";
    public static final boolean S3_ENABLE_PATH_STYLE_ACCESS_DEFAULT = false;
    public static final String S3_SDK_HTTP_CLIENT_BUILDER_CONFIG = S3_PREFIX + "sdk.http.client.builder";
    public static final String S3_SDK_HTTP_CLIENT_BUILDER_DOC = "The HTTP client to use for S3 client.";
    public static final Class<? extends SdkHttpClient.Builder> S3_SDK_HTTP_CLIENT_BUILDER_DEFAULT = null;
    public static final String S3_REGION_DEFAULT = "";
    public static final String S3_ACCESS_KEY_DOC = "AWS access key to use for connecting to S3. Leave empty if AWS"
            + " credential provider chain or STS Assume Role provider should be used.";
    public static final String S3_ACCESS_KEY_DEFAULT = "";
    public static final String S3_SECRET_KEY_DOC = "AWS secret key to use for connecting to S3. Leave empty if AWS"
            + " credential provider chain or STS Assume Role provider should be used.";
    public static final String S3_ROLE_EXTERNAL_ID_CONFIG_DOC = "AWS STS role external ID used when retrieving session"
            + " credentials under an assumed role. Leave empty if AWS Basic provider or AWS credential provider chain"
            + " should be used.";
    public static final String S3_ROLE_EXTERNAL_ID_CONFIG_DEFAULT = "";
    public static final String S3_ROLE_ARN_CONFIG_DOC = "AWS STS role ARN to use for connecting to S3. Leave empty if"
            + " AWS Basic provider or AWS credential provider chain should be used.";
    public static final String S3_ROLE_ARN_CONFIG_DEFAULT = "";
    public static final String S3_ROLE_SESSION_NAME_CONFIG_DOC = "AWS STS role session name to use when starting a"
            + " session. Leave empty if AWS Basic provider or AWS credential provider chain should be used.";
    public static final String S3_ROLE_SESSION_NAME_CONFIG_DEFAULT = "";
    public static final String S3_JWT_PATH_CONFIG_DOC =
            "Path to an OIDC token file in JSON format (JWT) used to authenticate before AWS STS role authorisation, "
                    + "e.g. for EKS `/var/run/secrets/eks.amazonaws.com/serviceaccount/token`.";
    public static final String S3_JWT_PATH_CONFIG_DEFAULT = "";
    public static final String S3_SECRET_KEY_DEFAULT = "";

    public static final String AZURE_PREFIX = PREFIX + AzureBlobStorageClient.SCHEME + ".";
    public static final String AZURE_CONNECTION_STRING_CONFIG = AZURE_PREFIX + "connection.string";
    public static final String AZURE_CONNECTION_STRING_DOC = "Azure connection string for connection to blob storage. "
            + "Leave empty if Azure credential provider chain should be used.";
    public static final String AZURE_CONNECTION_STRING_DEFAULT = "";

    public static final String GOOGLE_STORAGE_PREFIX = PREFIX + GoogleStorageClient.SCHEME + ".";
    public static final String GOOGLE_CLOUD_KEY_PATH = GOOGLE_STORAGE_PREFIX + "key.path";
    private static final ConfigDef config = baseConfigDef();
    public static final String GOOGLE_CLOUD_KEY_PATH_DOC = "Path to the service account JSON file";
    public static final String GOOGLE_CLOUD_KEY_PATH_DEFAULT = "";
    private static final String GOOGLE_CLOUD_OAUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform";
    private final Map<String, Supplier<BlobStorageClient>> clientFactories =
            ImmutableMap.<String, Supplier<BlobStorageClient>>builder()
                    .put(AmazonS3Client.SCHEME, this::createAmazonS3Client)
                    .put(AzureBlobStorageClient.SCHEME, this::createAzureBlobStorageClient)
                    .put(GoogleStorageClient.SCHEME, this::createGoogleStorageClient)
                    .build();

    /**
     * Create a new configuration from the given properties
     *
     * @param originals properties for configuring this config
     */
    public AbstractLargeMessageConfig(final Map<?, ?> originals) {
        super(config, originals);
    }

    protected AbstractLargeMessageConfig(final ConfigDef config, final Map<?, ?> originals) {
        super(config, originals);
    }

    protected static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(MAX_BYTE_SIZE_CONFIG, Type.INT, MAX_BYTE_SIZE_DEFAULT, Importance.MEDIUM, MAX_BYTE_SIZE_DOC)
                .define(BASE_PATH_CONFIG, Type.STRING, BASE_PATH_DEFAULT, Importance.HIGH, BASE_PATH_DOC)
                .define(USE_HEADERS_CONFIG, Type.BOOLEAN, USE_HEADERS_DEFAULT, Importance.MEDIUM, USE_HEADERS_DOC)
                .define(ACCEPT_NO_HEADERS_CONFIG, Type.BOOLEAN, ACCEPT_NO_HEADERS_DEFAULT, Importance.MEDIUM,
                        ACCEPT_NO_HEADERS_DOC)
                .define(ID_GENERATOR_CONFIG, Type.CLASS, ID_GENERATOR_DEFAULT, Importance.MEDIUM, ID_GENERATOR_DOC)
                .define(COMPRESSION_TYPE_CONFIG, Type.STRING, COMPRESSION_TYPE_DEFAULT, Importance.MEDIUM,
                        COMPRESSION_TYPE_DOC)
                // Amazon S3
                .define(S3_ENDPOINT_CONFIG, Type.STRING, S3_ENDPOINT_DEFAULT, Importance.LOW, S3_ENDPOINT_DOC)
                .define(S3_ENABLE_PATH_STYLE_ACCESS_CONFIG, Type.BOOLEAN, S3_ENABLE_PATH_STYLE_ACCESS_DEFAULT,
                        Importance.LOW, S3_ENABLE_PATH_STYLE_ACCESS_DOC)
                .define(S3_SDK_HTTP_CLIENT_BUILDER_CONFIG, Type.CLASS, S3_SDK_HTTP_CLIENT_BUILDER_DEFAULT, Importance.LOW, S3_SDK_HTTP_CLIENT_BUILDER_DOC)
                .define(S3_REGION_CONFIG, Type.STRING, S3_REGION_DEFAULT, Importance.LOW, S3_REGION_DOC)
                .define(S3_ACCESS_KEY_CONFIG, Type.PASSWORD, S3_ACCESS_KEY_DEFAULT, Importance.LOW, S3_ACCESS_KEY_DOC)
                .define(S3_SECRET_KEY_CONFIG, Type.PASSWORD, S3_SECRET_KEY_DEFAULT, Importance.LOW, S3_SECRET_KEY_DOC)
                .define(S3_ROLE_EXTERNAL_ID_CONFIG, Type.STRING, S3_ROLE_EXTERNAL_ID_CONFIG_DEFAULT, Importance.LOW,
                        S3_ROLE_EXTERNAL_ID_CONFIG_DOC)
                .define(S3_ROLE_ARN_CONFIG, Type.STRING, S3_ROLE_ARN_CONFIG_DEFAULT, Importance.LOW,
                        S3_ROLE_ARN_CONFIG_DOC)
                .define(S3_ROLE_SESSION_NAME_CONFIG, Type.STRING, S3_ROLE_SESSION_NAME_CONFIG_DEFAULT, Importance.LOW,
                        S3_ROLE_SESSION_NAME_CONFIG_DOC)
                .define(S3_JWT_PATH_CONFIG, Type.STRING, S3_JWT_PATH_CONFIG_DEFAULT, Importance.LOW,
                        S3_JWT_PATH_CONFIG_DOC)
                // Azure Blob Storage
                .define(AZURE_CONNECTION_STRING_CONFIG, Type.PASSWORD, AZURE_CONNECTION_STRING_DEFAULT, Importance.LOW,
                        AZURE_CONNECTION_STRING_DOC)
                // Google Cloud Storage
                .define(GOOGLE_CLOUD_KEY_PATH, Type.STRING, GOOGLE_CLOUD_KEY_PATH_DEFAULT, Importance.LOW,
                        GOOGLE_CLOUD_KEY_PATH_DOC)
                ;
    }

    static SerializationException unknownScheme(final String scheme) {
        return new SerializationException("Unknown scheme for handling large messages: '" + scheme + "'");
    }

    private static NoBlobStorageClient createNoBlobStorageClient() {
        log.warn("No " + BASE_PATH_CONFIG + " has been provided and storing a large message will lead to an error.");
        return new NoBlobStorageClient();
    }

    public LargeMessageRetrievingClient getRetriever() {
        return new LargeMessageRetrievingClient(this.clientFactories, this.getBoolean(ACCEPT_NO_HEADERS_CONFIG));
    }

    public LargeMessageStoringClient getStorer() {
        final BlobStorageClient client = this.getClient();
        return LargeMessageStoringClient.builder()
                .client(client)
                .basePath(this.getBasePath().orElse(null))
                .maxSize(this.getMaxSize())
                .idGenerator(this.getConfiguredInstance(ID_GENERATOR_CONFIG, IdGenerator.class))
                .protocol(this.getBoolean(USE_HEADERS_CONFIG) ? new HeaderLargeMessagePayloadProtocol()
                        : new ByteFlagLargeMessagePayloadProtocol())
                .compressionType(this.getCompressionType())
                .build();
    }

    protected <T> T getInstance(final String key, final Class<T> targetClass) {
        final Class<?> configuredClass = this.getClass(key);
        if (configuredClass == null) {
            return null;
        }
        final Object o = Utils.newInstance(configuredClass);
        if (!targetClass.isInstance(o)) {
            throw new KafkaException(configuredClass.getName() + " is not an instance of " + targetClass.getName());
        }
        return targetClass.cast(o);
    }

    private BlobStorageClient getClient() {
        return this.getBasePath()
                .map(BlobStorageURI::getScheme)
                .map(this::createClient)
                .orElseGet(AbstractLargeMessageConfig::createNoBlobStorageClient);
    }

    private BlobStorageClient createClient(final String scheme) {
        return Optional.ofNullable(this.clientFactories.get(scheme))
                .map(Supplier::get)
                .orElseThrow(() -> unknownScheme(scheme));
    }

    private Optional<BlobStorageURI> getBasePath() {
        final String basePath = this.getString(BASE_PATH_CONFIG);
        return isEmpty(basePath) ? Optional.empty() : Optional.of(BlobStorageURI.create(basePath));
    }

    private CompressionType getCompressionType() {
        return CompressionType.forName(this.getString(COMPRESSION_TYPE_CONFIG));
    }

    private int getMaxSize() {
        return this.getInt(MAX_BYTE_SIZE_CONFIG);
    }

    private BlobStorageClient createAzureBlobStorageClient() {
        final BlobServiceClientBuilder clientBuilder = new BlobServiceClientBuilder();
        this.getAzureConnectionString().ifPresent(clientBuilder::connectionString);
        final BlobServiceClient blobServiceClient = clientBuilder.buildClient();
        return new AzureBlobStorageClient(blobServiceClient);
    }

    private Optional<String> getAzureConnectionString() {
        final String connectionString = this.getPassword(AZURE_CONNECTION_STRING_CONFIG).value();
        return isEmpty(connectionString) ? Optional.empty() : Optional.of(connectionString);
    }

    private BlobStorageClient createAmazonS3Client() {
        final S3ClientBuilder clientBuilder = S3Client.builder();
        this.getAmazonEndpointOverride().ifPresent(clientBuilder::endpointOverride);
        this.getAmazonRegion().ifPresent(clientBuilder::region);
        this.getAmazonCredentialsProvider().ifPresent(clientBuilder::credentialsProvider);
        this.getAmazonSdkHttpClientBuilderInstance()
                .ifPresent(clientBuilder::httpClientBuilder);
        if (this.enableAmazonS3PathStyleAccess()) {
            clientBuilder.forcePathStyle(true);
        }
        return new AmazonS3Client(clientBuilder.build());
    }

    private <T extends SdkHttpClient.Builder<T>> Optional<SdkHttpClient.Builder<T>> getAmazonSdkHttpClientBuilderInstance() {
        final SdkHttpClient.Builder<T> builder = this.getInstance(AbstractLargeMessageConfig.S3_SDK_HTTP_CLIENT_BUILDER_CONFIG, SdkHttpClient.Builder.class);
        return Optional.ofNullable(builder);
    }

    private Optional<URI> getAmazonEndpointOverride() {
        final String endpoint = this.getString(S3_ENDPOINT_CONFIG);
        return isEmpty(endpoint) ? Optional.empty() : Optional.of(URI.create(endpoint));
    }

    private boolean enableAmazonS3PathStyleAccess() {
        return this.getBoolean(S3_ENABLE_PATH_STYLE_ACCESS_CONFIG);
    }

    private Optional<Region> getAmazonRegion() {
        final String region = this.getString(S3_REGION_CONFIG);
        return isEmpty(region) ? Optional.empty() : Optional.of(Region.of(region));
    }

    private Optional<AwsCredentialsProvider> getAmazonCredentialsProvider() {
        final String accessKey = this.getPassword(S3_ACCESS_KEY_CONFIG).value();
        final String secretKey = this.getPassword(S3_SECRET_KEY_CONFIG).value();

        if (!isEmpty(accessKey) && !isEmpty(secretKey)) {
            final AwsCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
            return Optional.of(StaticCredentialsProvider.create(credentials));
        }

        final String roleExternalId = this.getString(S3_ROLE_EXTERNAL_ID_CONFIG);
        final String roleArn = this.getString(S3_ROLE_ARN_CONFIG);
        final String roleSessionName = this.getString(S3_ROLE_SESSION_NAME_CONFIG);
        final String jwtPath = this.getString(S3_JWT_PATH_CONFIG);

        if (!isEmpty(roleArn) && !isEmpty(roleSessionName)) {

            if (!isEmpty(roleExternalId)) {
                final AwsCredentialsProvider roleProvider = StsAssumeRoleCredentialsProvider.builder()
                        .refreshRequest(builder -> builder
                                .roleArn(roleArn)
                                .roleSessionName(roleSessionName)
                                .externalId(roleExternalId))
                        .stsClient(StsClient.create())
                        .build();

                return Optional.of(roleProvider);
            }

            if (!isEmpty(jwtPath)) {
                final AwsCredentialsProvider oidcProvider = WebIdentityTokenFileCredentialsProvider.builder()
                        .webIdentityTokenFile(new File(jwtPath).toPath())
                        .roleArn(roleArn)
                        .roleSessionName(roleSessionName)
                        .build();

                return Optional.of(oidcProvider);
            }

        }

        return Optional.empty();
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
    private BlobStorageClient createGoogleStorageClient() {
        if (!this.getString(GOOGLE_CLOUD_KEY_PATH).isEmpty()) {
            final GoogleCredentials credentials = this.getGoogleCredentials();
            return new GoogleStorageClient(
                    StorageOptions.newBuilder().setCredentials(credentials).build().getService());
        }
        return new GoogleStorageClient(StorageOptions.getDefaultInstance().getService());
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
