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
import static software.amazon.awssdk.utils.StringUtils.isEmpty;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

/**
 * This class provides default configuration options for S3 backed data. It offers configuration of the following
 * properties:
 * <ul>
 *     <li> S3 endpoint
 *     <li> S3 region
 *     <li> S3 access key
 *     <li> S3 secret key
 *     <li> AWS security token service
 *     <li> AWS OIDC token path
 *     <li> S3 request checksum calculation mode
 * </ul>
 */
@Slf4j
public class AmazonS3Config extends AbstractConfig implements BlobStorageConfig {
    public static final String S3_PREFIX = PREFIX + AmazonS3Client.SCHEME + ".";
    public static final String S3_ENDPOINT_CONFIG = S3_PREFIX + "endpoint";
    public static final String S3_REGION_CONFIG = S3_PREFIX + "region";
    public static final String S3_ACCESS_KEY_CONFIG = S3_PREFIX + "access.key";
    public static final String S3_SECRET_KEY_CONFIG = S3_PREFIX + "secret.key";
    public static final String S3_ROLE_EXTERNAL_ID_CONFIG = S3_PREFIX + "sts.role.external.id";
    public static final String S3_ROLE_ARN_CONFIG = S3_PREFIX + "sts.role.arn";
    public static final String S3_ROLE_SESSION_NAME_CONFIG = S3_PREFIX + "sts.role.session.name";
    public static final String S3_JWT_PATH_CONFIG = S3_PREFIX + "jwt.path";
    public static final String S3_REQUEST_CHECKSUM_CALCULATION_CONFIG = S3_PREFIX + "request.checksum.calculation";
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
    public static final String S3_REQUEST_CHECKSUM_CALCULATION_DOC =
            "AWS request checksum validation mode to use when uploading to S3. Leave empty to use the AWS SDK default.";

    private static final ConfigDef config = baseConfigDef();

    static {
        AbstractLargeMessageConfig.register(AmazonS3Client.SCHEME, AmazonS3Config::new);
    }

    /**
     * Create a new configuration from the given properties
     *
     * @param originals properties for configuring this config
     */
    public AmazonS3Config(final Map<?, ?> originals) {
        super(config, originals);
    }

    protected AmazonS3Config(final ConfigDef config, final Map<?, ?> originals) {
        super(config, originals);
    }

    protected static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(S3_ENDPOINT_CONFIG, Type.STRING, S3_ENDPOINT_DEFAULT, Importance.LOW, S3_ENDPOINT_DOC)
                .define(S3_ENABLE_PATH_STYLE_ACCESS_CONFIG, Type.BOOLEAN, S3_ENABLE_PATH_STYLE_ACCESS_DEFAULT,
                        Importance.LOW, S3_ENABLE_PATH_STYLE_ACCESS_DOC)
                .define(S3_SDK_HTTP_CLIENT_BUILDER_CONFIG, Type.CLASS, S3_SDK_HTTP_CLIENT_BUILDER_DEFAULT,
                        Importance.LOW, S3_SDK_HTTP_CLIENT_BUILDER_DOC)
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
                .define(S3_REQUEST_CHECKSUM_CALCULATION_CONFIG, Type.STRING, null,
                        Importance.LOW, S3_REQUEST_CHECKSUM_CALCULATION_DOC)
                ;
    }

    @Override
    public BlobStorageClient createBlobStorageClient() {
        final S3ClientBuilder clientBuilder = S3Client.builder();
        this.getAmazonEndpointOverride().ifPresent(clientBuilder::endpointOverride);
        this.getAmazonRegion().ifPresent(clientBuilder::region);
        this.getAmazonCredentialsProvider().ifPresent(clientBuilder::credentialsProvider);
        this.getAmazonSdkHttpClientBuilderInstance()
                .ifPresent(clientBuilder::httpClientBuilder);
        if (this.enableAmazonS3PathStyleAccess()) {
            clientBuilder.forcePathStyle(true);
        }
        this.getAmazonRequestChecksumCalculation().ifPresent(clientBuilder::requestChecksumCalculation);
        return new AmazonS3Client(clientBuilder.build());
    }

    protected <T> T getInstance(final String key, final Class<T> targetClass) {
        return AbstractLargeMessageConfig.getInstance(this, key, targetClass);
    }

    private <T extends SdkHttpClient.Builder<T>> Optional<SdkHttpClient.Builder<T>> getAmazonSdkHttpClientBuilderInstance() {
        final SdkHttpClient.Builder<T> builder =
                this.getInstance(S3_SDK_HTTP_CLIENT_BUILDER_CONFIG, SdkHttpClient.Builder.class);
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

    private Optional<RequestChecksumCalculation> getAmazonRequestChecksumCalculation() {
        final String requestChecksumCalculation = this.getString(S3_REQUEST_CHECKSUM_CALCULATION_CONFIG);
        return isEmpty(requestChecksumCalculation) ? Optional.empty()
                : Optional.of(RequestChecksumCalculation.fromValue(requestChecksumCalculation));
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
}
