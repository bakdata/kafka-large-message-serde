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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Value;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

class AmazonS3LargeMessageClientRoundtripTest extends AmazonS3IntegrationTest {

    private static final String TOPIC = "output";
    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();

    private static byte[] serialize(final String s) {
        return STRING_SERIALIZER.serialize(null, s);
    }

    private static Stream<Arguments> provideParameters() {
        return Stream.of(true, false)
                .flatMap(isKey -> Stream.of(true, false)
                        .map(enabledPathAccess -> RoundtripArgument.builder()
                                .isKey(isKey)
                                .isPathStyleAccess(enabledPathAccess)))
                .flatMap(builder -> Stream.of("none", "gzip", "snappy", "lz4", "zstd")
                        .map(c -> builder.compressionType(c).build())
                        .map(Arguments::of));
    }

    @ParameterizedTest
    @MethodSource("provideParameters")
    void shouldRoundtrip(final RoundtripArgument argument) {
        final String bucket = "bucket";
        final String basePath = "s3://" + bucket + "/base/";
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, 0)
                .put(AbstractLargeMessageConfig.BASE_PATH_CONFIG, basePath)
                .put(AbstractLargeMessageConfig.S3_ENABLE_PATH_STYLE_ACCESS_CONFIG, argument.isPathStyleAccess())
                .put(AbstractLargeMessageConfig.COMPRESSION_TYPE_CONFIG, argument.getCompressionType())
                .build();
        final S3Client s3 = this.getS3Client();
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        try (final LargeMessageStoringClient storer = this.createStorer(properties);
                final LargeMessageRetrievingClient retriever = this.createRetriever()) {

            final Headers headers = new RecordHeaders();
            final byte[] obj = serialize("big value");
            final byte[] data = storer.storeBytes(TOPIC, obj, argument.isKey(), headers);

            final Iterable<Header> compressionHeaders = headers.headers(CompressionType.HEADER_NAME);
            if ("none".equals(argument.getCompressionType())) {
                assertThat(compressionHeaders).isEmpty();
            } else {
                assertThat(compressionHeaders).isNotEmpty();
            }

            final byte[] result = retriever.retrieveBytes(data, headers, argument.isKey());
            assertThat(result).isEqualTo(obj);
        }
    }

    private Map<String, Object> createStorerProperties(final Map<String, Object> properties) {
        return ImmutableMap.<String, Object>builder()
                .putAll(properties)
                .putAll(this.getLargeMessageConfig())
                .build();
    }

    private LargeMessageStoringClient createStorer(final Map<String, Object> baseProperties) {
        final Map<String, Object> properties = this.createStorerProperties(baseProperties);
        final AbstractLargeMessageConfig config = new AbstractLargeMessageConfig(properties);
        return config.getStorer();
    }

    private LargeMessageRetrievingClient createRetriever() {
        final Map<String, String> properties = this.getLargeMessageConfig();
        final AbstractLargeMessageConfig config = new AbstractLargeMessageConfig(properties);
        return config.getRetriever();
    }

    @Builder
    @Value
    static class RoundtripArgument {
        boolean isKey;
        boolean isPathStyleAccess;
        String compressionType;
    }
}
