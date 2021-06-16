/*
 * MIT License
 *
 * Copyright (c) 2020 bakdata
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

import static org.apache.http.util.TextUtils.isEmpty;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigDef.Importance;
import io.confluent.common.config.ConfigDef.Type;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * This class provides default configuration options for blob storage backed data. It offers configuration of the
 * following properties:
 * <p>
 * <ul>
 *     <li> maximum serialized message size in bytes
 *     <li> base path
 *     <li> id generator
 *     <li> S3 endpoint
 *     <li> S3 region
 *     <li> S3 access key
 *     <li> S3 secret key
 *     <li> AWS security token service
 *     <li> S3 enable path-style access
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
            + "should be used, e.g., 's3://my-bucket/my/prefix/'. Available protocols: 's3', 'wasbs'";
    public static final String BASE_PATH_DEFAULT = "";
    public static final String ID_GENERATOR_CONFIG = PREFIX + "id.generator";
    public static final String ID_GENERATOR_DOC = "Class to use for generating unique object IDs. Available "
            + "generators are: " + RandomUUIDGenerator.class.getName() + ", " + Sha256HashIdGenerator.class.getName()
            + ", " + MurmurHashIdGenerator.class.getName() + ".";
    public static final Class<? extends IdGenerator> ID_GENERATOR_DEFAULT = RandomUUIDGenerator.class;
    public static final String S3_PREFIX = PREFIX + "s3.";
    public static final String S3_ENDPOINT_CONFIG = S3_PREFIX + "endpoint";
    public static final String S3_ENDPOINT_DEFAULT = "";
    public static final String S3_REGION_CONFIG = S3_PREFIX + "region";
    public static final String S3_ENDPOINT_DOC = "Endpoint to use for connection to Amazon S3. Must be configured in"
            + " conjunction with " + S3_REGION_CONFIG + ". Leave empty if default S3 endpoint should be used.";
    public static final String S3_REGION_DOC = "S3 region to use. Must be configured in conjunction"
            + " with " + S3_ENDPOINT_CONFIG + ". Leave empty if default S3 region should be used.";
    public static final String S3_REGION_DEFAULT = "";
    public static final String S3_ACCESS_KEY_CONFIG = S3_PREFIX + "access.key";
    public static final String S3_ACCESS_KEY_DOC = "AWS access key to use for connecting to S3. Leave empty if AWS"
            + " credential provider chain or STS Assume Role provider should be used.";
    public static final String S3_ACCESS_KEY_DEFAULT = "";
    public static final String S3_SECRET_KEY_CONFIG = S3_PREFIX + "secret.key";
    public static final String S3_SECRET_KEY_DOC = "AWS secret key to use for connecting to S3. Leave empty if AWS"
            + " credential provider chain or STS Assume Role provider should be used.";
    public static final String S3_ROLE_EXTERNAL_ID_CONFIG = S3_PREFIX + "sts.role.external.id";
    public static final String S3_ROLE_EXTERNAL_ID_CONFIG_DOC = "AWS STS role external ID used when retrieving session"
            + " credentials under an assumed role. Leave empty if AWS Basic provider or AWS credential provider chain"
            + " should be used.";
    public static final String S3_ROLE_EXTERNAL_ID_CONFIG_DEFAULT = "";
    public static final String S3_ROLE_ARN_CONFIG = S3_PREFIX + "sts.role.arn";
    public static final String S3_ROLE_ARN_CONFIG_DOC = "AWS STS role ARN to use for connecting to S3. Leave empty if"
            + " AWS Basic provider or AWS credential provider chain should be used.";
    public static final String S3_ROLE_ARN_CONFIG_DEFAULT = "";
    public static final String S3_ROLE_SESSION_NAME_CONFIG = S3_PREFIX + "sts.role.session.name";
    public static final String S3_ROLE_SESSION_NAME_CONFIG_DOC = "AWS STS role session name to use when starting a"
            + " session. Leave empty if AWS Basic provider or AWS credential provider chain should be used.";
    public static final String S3_ROLE_SESSION_NAME_CONFIG_DEFAULT = "";
    public static final String S3_SECRET_KEY_DEFAULT = "";
    public static final String S3_ENABLE_PATH_STYLE_ACCESS_CONFIG = S3_PREFIX + "path.style.access";
    public static final String S3_ENABLE_PATH_STYLE_ACCESS_DOC = "Enable path-style access for S3 client.";
    public static final boolean S3_ENABLE_PATH_STYLE_ACCESS_DEFAULT = false;
    private static final ConfigDef config = baseConfigDef();

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
                .define(ID_GENERATOR_CONFIG, Type.CLASS, ID_GENERATOR_DEFAULT, Importance.MEDIUM, ID_GENERATOR_DOC)
                .define(S3_ENDPOINT_CONFIG, Type.STRING, S3_ENDPOINT_DEFAULT, Importance.LOW, S3_ENDPOINT_DOC)
                .define(S3_REGION_CONFIG, Type.STRING, S3_REGION_DEFAULT, Importance.LOW, S3_REGION_DOC)
                .define(S3_ACCESS_KEY_CONFIG, Type.PASSWORD, S3_ACCESS_KEY_DEFAULT, Importance.LOW, S3_ACCESS_KEY_DOC)
                .define(S3_SECRET_KEY_CONFIG, Type.PASSWORD, S3_SECRET_KEY_DEFAULT, Importance.LOW, S3_SECRET_KEY_DOC)
                .define(S3_ENABLE_PATH_STYLE_ACCESS_CONFIG, Type.BOOLEAN, S3_ENABLE_PATH_STYLE_ACCESS_DEFAULT,
                        Importance.LOW, S3_ENABLE_PATH_STYLE_ACCESS_DOC)
                .define(S3_ROLE_EXTERNAL_ID_CONFIG, Type.STRING, S3_ROLE_EXTERNAL_ID_CONFIG_DEFAULT, Importance.LOW,
                        S3_ROLE_EXTERNAL_ID_CONFIG_DOC)
                .define(S3_ROLE_ARN_CONFIG, Type.STRING, S3_ROLE_ARN_CONFIG_DEFAULT, Importance.LOW,
                        S3_ROLE_ARN_CONFIG_DOC)
                .define(S3_ROLE_SESSION_NAME_CONFIG, Type.STRING, S3_ROLE_SESSION_NAME_CONFIG_DEFAULT, Importance.LOW,
                        S3_ROLE_SESSION_NAME_CONFIG_DOC);
    }

    public LargeMessageRetrievingClient getRetriever() {
        final BlobStorageClient client = this.getClient();
        return new LargeMessageRetrievingClient(client);
    }

    public LargeMessageStoringClient getStorer() {
        final BlobStorageClient client = this.getClient();
        return LargeMessageStoringClient.builder()
                .client(client)
                .basePath(this.getBasePath())
                .maxSize(this.getMaxSize())
                .idGenerator(this.getConfiguredInstance(ID_GENERATOR_CONFIG, IdGenerator.class))
                .build();
    }

    private BlobStorageClient getClient() {
        final Optional<URI> uri = this.getBasePathUri();
        return uri.map(this::getClient)
                .orElseGet(MissingStorageClient::new);
    }

    private BlobStorageClient getClient(final URI uri) {
        final String scheme = uri.getScheme();
        switch (scheme) {
            case AmazonS3Client.SCHEME:
                return this.createS3Client();
            case AzureBlobStorageClient.SCHEME:
                return this.createAzureClient();
            default:
                throw new IllegalArgumentException("Unknown scheme for handling large messages: '" + scheme + "'");
        }
    }

    private BlobStorageClient createAzureClient() {
        final BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().buildClient();
        return new AzureBlobStorageClient(blobServiceClient);
    }

    private BlobStorageClient createS3Client() {
        final AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard();
        this.getEndpointConfiguration().ifPresent(clientBuilder::setEndpointConfiguration);
        this.getCredentialsProvider().ifPresent(clientBuilder::setCredentials);
        if (this.enablePathStyleAccess()) {
            clientBuilder.enablePathStyleAccess();
        }
        return new AmazonS3Client(clientBuilder.build());
    }

    private BlobStorageURI getBasePath() {
        return this.getBasePathUri().map(BlobStorageURI::new).orElse(null);
    }

    private Optional<URI> getBasePathUri() {
        final String basePath = this.getString(BASE_PATH_CONFIG);
        return isEmpty(basePath) ? Optional.empty() : Optional.of(URI.create(basePath));
    }

    private int getMaxSize() {
        return this.getInt(MAX_BYTE_SIZE_CONFIG);
    }

    private boolean enablePathStyleAccess() {
        return this.getBoolean(S3_ENABLE_PATH_STYLE_ACCESS_CONFIG);
    }

    private Optional<EndpointConfiguration> getEndpointConfiguration() {
        final String endpoint = this.getString(S3_ENDPOINT_CONFIG);
        final String region = this.getString(S3_REGION_CONFIG);
        return isEmpty(endpoint) || isEmpty(region) ? Optional.empty()
                : Optional.of(new EndpointConfiguration(endpoint, region));
    }

    private Optional<AWSCredentialsProvider> getCredentialsProvider() {
        final String accessKey = this.getPassword(S3_ACCESS_KEY_CONFIG).value();
        final String secretKey = this.getPassword(S3_SECRET_KEY_CONFIG).value();

        if (!isEmpty(accessKey) && !isEmpty(secretKey)) {
            final AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
            return Optional.of(new AWSStaticCredentialsProvider(credentials));
        }

        final String roleExternalId = this.getString(S3_ROLE_EXTERNAL_ID_CONFIG);
        final String roleArn = this.getString(S3_ROLE_ARN_CONFIG);
        final String roleSessionName = this.getString(S3_ROLE_SESSION_NAME_CONFIG);

        if (!isEmpty(roleExternalId) && !isEmpty(roleArn) && !isEmpty(roleSessionName)) {
            final AWSCredentialsProvider provider =
                    new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, roleSessionName)
                            .withStsClient(AWSSecurityTokenServiceClientBuilder.defaultClient())
                            .withExternalId(roleExternalId)
                            .build();

            return Optional.of(provider);
        }

        return Optional.empty();
    }

}
