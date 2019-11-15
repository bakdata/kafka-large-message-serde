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

import static org.apache.http.util.TextUtils.isEmpty;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigDef.Importance;
import io.confluent.common.config.ConfigDef.Type;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;

/**
 * This class provides configuration options for {@link S3BackedSerde}. It offers configuration of the following
 * properties:
 * <p>
 * <ul>
 *     <li> S3 endpoint
 *     <li> S3 region
 *     <li> S3 access key
 *     <li> S3 secret key
 *     <li> S3 enable path-style access
 *     <li> key serde class
 *     <li> value serde class
 *     <li> maximum message size
 *     <li> S3 base path
 * </ul>
 */
public class S3BackedSerdeConfig extends AbstractConfig {
    public static final String PREFIX = "s3backed.";
    public static final String S3_ENDPOINT_CONFIG = PREFIX + "endpoint";
    public static final String S3_ENDPOINT_DEFAULT = "";
    public static final String S3_REGION_CONFIG = PREFIX + "region";
    public static final String S3_ENDPOINT_DOC = "Endpoint to use for connection to Amazon S3. Must be configured in"
            + " conjunction with " + S3_REGION_CONFIG + ". Leave empty if default S3 endpoint should be used.";
    public static final String S3_REGION_DOC = "S3 region to use. Must be configured in conjunction"
            + " with " + S3_ENDPOINT_CONFIG + ". Leave empty if default S3 region should be used.";
    public static final String S3_REGION_DEFAULT = "";
    public static final String S3_ACCESS_KEY_CONFIG = PREFIX + "access.key";
    public static final String S3_ACCESS_KEY_DOC =
            "AWS access key to use for connecting to S3. Leave empty if AWS credential provider chain should be used.";
    public static final String S3_ACCESS_KEY_DEFAULT = "";
    public static final String S3_SECRET_KEY_CONFIG = PREFIX + "secret.key";
    public static final String S3_SECRET_KEY_DOC =
            "AWS secret key to use for connecting to S3. Leave empty if AWS credential provider chain should be used.";
    public static final String S3_SECRET_KEY_DEFAULT = "";
    public static final String S3_ENABLE_PATH_STYLE_ACCESS_CONFIG = PREFIX + "path.style.access";
    public static final String S3_ENABLE_PATH_STYLE_ACCESS_DOC = "Enable path-style access for S3 client.";
    public static final boolean S3_ENABLE_PATH_STYLE_ACCESS_DEFAULT = false;
    public static final String KEY_SERDE_CLASS_CONFIG = PREFIX + "key.serde";
    public static final String KEY_SERDE_CLASS_DOC = "Key serde class to use.";
    public static final String VALUE_SERDE_CLASS_CONFIG = PREFIX + "value.serde";
    public static final Class<? extends Serde<?>> KEY_SERDE_CLASS_DEFAULT = ByteArraySerde.class;
    public static final String VALUE_SERDE_CLASS_DOC = "Value serde class to use.";
    public static final Class<? extends Serde<?>> VALUE_SERDE_CLASS_DEFAULT = ByteArraySerde.class;
    public static final String MAX_BYTE_SIZE_CONFIG = PREFIX + "max.byte.size";
    public static final String MAX_BYTE_SIZE_DOC =
            "Maximum message size in bytes before serialized messages are stored on S3.";
    public static final int MAX_BYTE_SIZE_DEFAULT = 1000 * 1000;
    public static final String BASE_PATH_CONFIG = PREFIX + "base.path";
    public static final String BASE_PATH_DOC = "Base path to store data. Must include bucket and any prefix that "
            + "should be used, e.g., 's3://my-bucket/my/prefix/'.";
    public static final String BASE_PATH_DEFAULT = "";
    private static final ConfigDef config = baseConfigDef();

    protected S3BackedSerdeConfig(final Map<?, ?> originals) {
        super(config, originals);
    }

    private static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(S3_ENDPOINT_CONFIG, Type.STRING, S3_ENDPOINT_DEFAULT, Importance.LOW, S3_ENDPOINT_DOC)
                .define(S3_REGION_CONFIG, Type.STRING, S3_REGION_DEFAULT, Importance.LOW, S3_REGION_DOC)
                .define(KEY_SERDE_CLASS_CONFIG, Type.CLASS, KEY_SERDE_CLASS_DEFAULT, Importance.HIGH,
                        KEY_SERDE_CLASS_DOC)
                .define(VALUE_SERDE_CLASS_CONFIG, Type.CLASS, VALUE_SERDE_CLASS_DEFAULT, Importance.HIGH,
                        VALUE_SERDE_CLASS_DOC)
                .define(S3_ACCESS_KEY_CONFIG, Type.PASSWORD, S3_ACCESS_KEY_DEFAULT, Importance.LOW, S3_ACCESS_KEY_DOC)
                .define(S3_SECRET_KEY_CONFIG, Type.PASSWORD, S3_SECRET_KEY_DEFAULT, Importance.LOW, S3_SECRET_KEY_DOC)
                .define(S3_ENABLE_PATH_STYLE_ACCESS_CONFIG, Type.BOOLEAN, S3_ENABLE_PATH_STYLE_ACCESS_DEFAULT,
                        Importance.LOW, S3_ENABLE_PATH_STYLE_ACCESS_DOC)
                .define(MAX_BYTE_SIZE_CONFIG, Type.INT, MAX_BYTE_SIZE_DEFAULT, Importance.MEDIUM, MAX_BYTE_SIZE_DOC)
                .define(BASE_PATH_CONFIG, Type.STRING, BASE_PATH_DEFAULT, Importance.HIGH, BASE_PATH_DOC);
    }

    public S3RetrievingClient getS3Retriever() {
        final AmazonS3 s3 = this.createS3Client();
        return new S3RetrievingClient(s3);
    }

    public S3StoringClient getS3Storer() {
        final AmazonS3 s3 = this.createS3Client();
        return S3StoringClient.builder()
                .s3(s3)
                .basePath(this.getBasePath())
                .maxSize(this.getMaxSize())
                .build();
    }

    public <T> Serde<T> getKeySerde() {
        return (Serde<T>) this.getConfiguredInstance(KEY_SERDE_CLASS_CONFIG, Serde.class);
    }

    public <T> Serde<T> getValueSerde() {
        return (Serde<T>) this.getConfiguredInstance(VALUE_SERDE_CLASS_CONFIG, Serde.class);
    }

    private AmazonS3 createS3Client() {
        final AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard();
        this.getEndpointConfiguration().ifPresent(clientBuilder::setEndpointConfiguration);
        this.getCredentialsProvider().ifPresent(clientBuilder::setCredentials);
        if (this.enablePathStyleAccess()) {
            clientBuilder.enablePathStyleAccess();
        }
        return clientBuilder.build();
    }

    private AmazonS3URI getBasePath() {
        final String basePath = this.getString(BASE_PATH_CONFIG);
        return isEmpty(basePath) ? null : new AmazonS3URI(basePath);
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
        if (isEmpty(accessKey) || isEmpty(secretKey)) {
            return Optional.empty();
        } else {
            final AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
            return Optional.of(new AWSStaticCredentialsProvider(credentials));
        }
    }
}
