package com.bakdata.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.common.collect.ImmutableMap;

import io.confluent.common.config.ConfigDef;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class AmazonS3LargeMessageClientRoundtripTest extends AmazonS3ClientIntegrationTest {

    private static final String TOPIC = "output";
    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();

    private static byte[] serialize(final String s) {
        return STRING_SERIALIZER.serialize(null, s);
    }

    @ParameterizedTest
    @MethodSource("provideParameters")
    void shouldRoundtrip(final boolean isKey, final String compressionType) {
        final String bucket = "bucket";
        final String basePath = "s3://" + bucket + "/base/";
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, 0)
                .put(AbstractLargeMessageConfig.BASE_PATH_CONFIG, basePath)
                .put(AbstractLargeMessageConfig.COMPRESSION_TYPE_CONFIG, compressionType)
                .build();
        final S3Client s3 = this.getS3Client();
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        final LargeMessageStoringClient storer = this.createStorer(properties);

        final LargeMessageRetrievingClient retriever = this.createRetriever();

        RecordHeaders headers = new RecordHeaders();
        byte[] obj = serialize("big value");
        byte[] data = storer.storeBytes(TOPIC, obj, isKey, headers);

        Iterable<Header> compressionHeaders = headers.headers(CompressionType.HEADER_NAME);
        if (compressionType == "none") {
            assertThat(compressionHeaders).isEmpty();
        } else {
            assertThat(compressionHeaders).isNotEmpty();
        }

        byte[] result = retriever.retrieveBytes(data, headers, isKey);
        assertArrayEquals(obj, result);
    }

    private static Stream<Arguments> provideParameters() {
        return Stream.of(true, false).flatMap(isKey -> Stream.of("none", "gzip", "snappy", "lz4", "zstd").map(c -> Arguments.of(isKey, c)));
    }

    private Map<String, Object> createStorerProperties(final Map<String, Object> properties) {
        final AwsBasicCredentials credentials = this.getCredentials();
        return ImmutableMap.<String, Object>builder()
                .putAll(properties)
                .put(AbstractLargeMessageConfig.S3_ENDPOINT_CONFIG, this.getEndpointOverride().toString())
                .put(AbstractLargeMessageConfig.S3_REGION_CONFIG, this.getRegion().id())
                .put(AbstractLargeMessageConfig.S3_ACCESS_KEY_CONFIG, credentials.accessKeyId())
                .put(AbstractLargeMessageConfig.S3_SECRET_KEY_CONFIG, credentials.secretAccessKey())
                .build();
    }

    private LargeMessageStoringClient createStorer(final Map<String, Object> baseProperties) {
        final Map<String, Object> properties = this.createStorerProperties(baseProperties);
        final AbstractLargeMessageConfig config = new AbstractLargeMessageConfig(properties);
        return config.getStorer();
    }

    private Map<String, Object> createRetrieverProperties() {
        final AwsBasicCredentials credentials = this.getCredentials();
        return ImmutableMap.<String, Object>builder()
                .put(AbstractLargeMessageConfig.S3_ENDPOINT_CONFIG, this.getEndpointOverride().toString())
                .put(AbstractLargeMessageConfig.S3_REGION_CONFIG, this.getRegion().id())
                .put(AbstractLargeMessageConfig.S3_ACCESS_KEY_CONFIG, credentials.accessKeyId())
                .put(AbstractLargeMessageConfig.S3_SECRET_KEY_CONFIG, credentials.secretAccessKey())
                .build();
    }

    private LargeMessageRetrievingClient createRetriever() {
        final Map<String, Object> properties = this.createRetrieverProperties();
        final ConfigDef configDef = AbstractLargeMessageConfig.baseConfigDef();
        final AbstractLargeMessageConfig config = new AbstractLargeMessageConfig(configDef, properties);
        return config.getRetriever();
    }
}
