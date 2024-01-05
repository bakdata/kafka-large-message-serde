/*
 * MIT License
 *
 * Copyright (c) 2024 bakdata
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

import static com.bakdata.kafka.ByteFlagLargeMessagePayloadProtocol.stripFlag;
import static com.bakdata.kafka.HeaderLargeMessagePayloadProtocol.getHeaderName;
import static com.bakdata.kafka.LargeMessageRetrievingClient.deserializeUri;
import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.jooq.lambda.Seq;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.utils.IoUtils;

class LargeMessageSerializerTest extends AmazonS3IntegrationTest {

    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";
    private static final Deserializer<String> STRING_DESERIALIZER = Serdes.String().deserializer();
    private TestTopology<Integer, String> topology = null;

    private static BlobStorageURI deserializeUriWithFlag(final byte[] data) {
        final byte[] uriBytes = stripFlag(data);
        return deserializeUri(uriBytes);
    }

    private static Topology createValueTopology(final Properties properties) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Map<String, Object> configs = new StreamsConfig(properties).originals();
        final Serde<String> serde = new LargeMessageSerde<>();
        serde.configure(configs, false);
        final KStream<Integer, String> input =
                builder.stream(INPUT_TOPIC, Consumed.with(Serdes.Integer(), Serdes.String()));
        input.to(OUTPUT_TOPIC, Produced.with(Serdes.Integer(), serde));
        return builder.build();
    }

    private static Topology createKeyTopology(final Properties properties) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Map<String, Object> configs = new StreamsConfig(properties).originals();
        final Serde<String> serde = new LargeMessageSerde<>();
        serde.configure(configs, true);
        final KStream<String, Integer> input =
                builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.Integer()));
        input.to(OUTPUT_TOPIC, Produced.with(serde, Serdes.Integer()));
        return builder.build();
    }

    private static void expectNonBackedText(final String expected, final byte[] s3BackedText) {
        assertThat(STRING_DESERIALIZER.deserialize(null, stripFlag(s3BackedText)))
                .isInstanceOf(String.class)
                .isEqualTo(expected);
    }

    private static void expectNonBackedText(final String expected, final byte[] s3BackedText, final Headers headers,
            final boolean isKey) {
        assertThat(STRING_DESERIALIZER.deserialize(null, s3BackedText))
                .isInstanceOf(String.class)
                .isEqualTo(expected);
        assertThat(headers.headers(getHeaderName(isKey))).hasSize(1);
    }

    @AfterEach
    void tearDown() {
        if (this.topology != null) {
            this.topology.stop();
        }
    }

    @Test
    void shouldWriteNonBackedTextKey() {
        final Properties properties = new Properties();
        properties.put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, Integer.MAX_VALUE);
        this.createTopology(LargeMessageSerializerTest::createKeyTopology, properties);
        this.topology.input()
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Integer())
                .add("foo", 1);
        final List<ProducerRecord<byte[], Integer>> records = Seq.seq(this.topology.streamOutput()
                        .withKeySerde(Serdes.ByteArray())
                        .withValueSerde(Serdes.Integer()))
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::key)
                .anySatisfy(s3BackedText -> expectNonBackedText("foo", s3BackedText));
    }

    @Test
    void shouldWriteNonBackedTextKeyWithHeaders() {
        final Properties properties = new Properties();
        properties.put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, Integer.MAX_VALUE);
        properties.put(AbstractLargeMessageConfig.USE_HEADERS_CONFIG, true);
        this.createTopology(LargeMessageSerializerTest::createKeyTopology, properties);
        this.topology.input()
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Integer())
                .add("foo", 1);
        final List<ProducerRecord<byte[], Integer>> records = Seq.seq(this.topology.streamOutput()
                        .withKeySerde(Serdes.ByteArray())
                        .withValueSerde(Serdes.Integer()))
                .toList();
        assertThat(records)
                .hasSize(1)
                .anySatisfy(record -> expectNonBackedText("foo", record.key(), record.headers(), true));
    }

    @Test
    void shouldWriteNonBackedNullKey() {
        final Properties properties = new Properties();
        properties.put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, Integer.MAX_VALUE);
        this.createTopology(LargeMessageSerializerTest::createKeyTopology, properties);
        this.topology.input()
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Integer())
                .add(null, 1);
        final List<ProducerRecord<byte[], Integer>> records = Seq.seq(this.topology.streamOutput()
                        .withKeySerde(Serdes.ByteArray())
                        .withValueSerde(Serdes.Integer()))
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::key)
                .anySatisfy(s3BackedText -> assertThat(s3BackedText).isNull());
    }

    @Test
    void shouldWriteNonBackedTextValue() {
        final Properties properties = new Properties();
        properties.put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, Integer.MAX_VALUE);
        this.createTopology(LargeMessageSerializerTest::createValueTopology, properties);
        this.topology.input()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.String())
                .add(1, "foo");
        final List<ProducerRecord<Integer, byte[]>> records = Seq.seq(this.topology.streamOutput()
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(Serdes.ByteArray()))
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::value)
                .anySatisfy(s3BackedText -> expectNonBackedText("foo", s3BackedText));
    }

    @Test
    void shouldWriteNonBackedTextValueWithHeaders() {
        final Properties properties = new Properties();
        properties.put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, Integer.MAX_VALUE);
        properties.put(AbstractLargeMessageConfig.USE_HEADERS_CONFIG, true);
        this.createTopology(LargeMessageSerializerTest::createValueTopology, properties);
        this.topology.input()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.String())
                .add(1, "foo");
        final List<ProducerRecord<Integer, byte[]>> records = Seq.seq(this.topology.streamOutput()
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(Serdes.ByteArray()))
                .toList();
        assertThat(records)
                .hasSize(1)
                .anySatisfy(record -> expectNonBackedText("foo", record.value(), record.headers(), false));
    }

    @Test
    void shouldWriteNonBackedNullValue() {
        final Properties properties = new Properties();
        properties.put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, Integer.MAX_VALUE);
        this.createTopology(LargeMessageSerializerTest::createValueTopology, properties);
        this.topology.input()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.String())
                .add(1, null);
        final List<ProducerRecord<Integer, byte[]>> records = Seq.seq(this.topology.streamOutput()
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(Serdes.ByteArray()))
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::value)
                .anySatisfy(s3BackedText -> assertThat(s3BackedText).isNull());
    }

    @Test
    void shouldWriteBackedTextKey() {
        final String bucket = "bucket";
        final String basePath = "s3://" + bucket + "/base/";
        final Properties properties = new Properties();
        properties.put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, 0);
        properties.setProperty(AbstractLargeMessageConfig.BASE_PATH_CONFIG, basePath);
        this.createTopology(LargeMessageSerializerTest::createKeyTopology, properties);
        final S3Client s3Client = this.getS3Client();
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        this.topology.input()
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Integer())
                .add("foo", 1);
        final List<ProducerRecord<byte[], Integer>> records = Seq.seq(this.topology.streamOutput()
                        .withKeySerde(Serdes.ByteArray())
                        .withValueSerde(Serdes.Integer()))
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::key)
                .anySatisfy(s3BackedText -> expectBackedText(basePath, "foo", s3BackedText, "keys"));
    }

    @Test
    void shouldWriteBackedTextKeyWithHeaders() {
        final String bucket = "bucket";
        final String basePath = "s3://" + bucket + "/base/";
        final Properties properties = new Properties();
        properties.put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, 0);
        properties.setProperty(AbstractLargeMessageConfig.BASE_PATH_CONFIG, basePath);
        properties.put(AbstractLargeMessageConfig.USE_HEADERS_CONFIG, true);
        this.createTopology(LargeMessageSerializerTest::createKeyTopology, properties);
        final S3Client s3Client = this.getS3Client();
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        this.topology.input()
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Integer())
                .add("foo", 1);
        final List<ProducerRecord<byte[], Integer>> records = Seq.seq(this.topology.streamOutput()
                        .withKeySerde(Serdes.ByteArray())
                        .withValueSerde(Serdes.Integer()))
                .toList();
        assertThat(records)
                .hasSize(1)
                .anySatisfy(
                        record -> this.expectBackedText(basePath, "foo", record.key(), "keys", record.headers(), true));
    }

    @Test
    void shouldWriteBackedNullKey() {
        final Properties properties = new Properties();
        properties.put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, 0);
        this.createTopology(LargeMessageSerializerTest::createKeyTopology, properties);
        this.topology.input()
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Integer())
                .add(null, 1);
        final List<ProducerRecord<byte[], Integer>> records = Seq.seq(this.topology.streamOutput()
                        .withKeySerde(Serdes.ByteArray())
                        .withValueSerde(Serdes.Integer()))
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::key)
                .anySatisfy(s3BackedText -> assertThat(s3BackedText).isNull());
    }

    @Test
    void shouldWriteBackedTextValue() {
        final String bucket = "bucket";
        final String basePath = "s3://" + bucket + "/base/";
        final Properties properties = new Properties();
        properties.put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, 0);
        properties.setProperty(AbstractLargeMessageConfig.BASE_PATH_CONFIG, basePath);
        this.createTopology(LargeMessageSerializerTest::createValueTopology, properties);
        final S3Client s3Client = this.getS3Client();
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        this.topology.input()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.String())
                .add(1, "foo");
        final List<ProducerRecord<Integer, byte[]>> records = Seq.seq(this.topology.streamOutput()
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(Serdes.ByteArray()))
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::value)
                .anySatisfy(s3BackedText -> expectBackedText(basePath, "foo", s3BackedText, "values"));
    }

    @Test
    void shouldWriteBackedTextValueWithHeaders() {
        final String bucket = "bucket";
        final String basePath = "s3://" + bucket + "/base/";
        final Properties properties = new Properties();
        properties.put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, 0);
        properties.setProperty(AbstractLargeMessageConfig.BASE_PATH_CONFIG, basePath);
        properties.put(AbstractLargeMessageConfig.USE_HEADERS_CONFIG, true);
        this.createTopology(LargeMessageSerializerTest::createValueTopology, properties);
        final S3Client s3Client = this.getS3Client();
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        this.topology.input()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.String())
                .add(1, "foo");
        final List<ProducerRecord<Integer, byte[]>> records = Seq.seq(this.topology.streamOutput()
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(Serdes.ByteArray()))
                .toList();
        assertThat(records)
                .hasSize(1)
                .anySatisfy(
                        record -> this.expectBackedText(basePath, "foo", record.value(), "values", record.headers(),
                                false));
    }

    @Test
    void shouldWriteBackedNullValue() {
        final Properties properties = new Properties();
        properties.put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, 0);
        this.createTopology(LargeMessageSerializerTest::createValueTopology, properties);
        this.topology.input()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.String())
                .add(1, null);
        final List<ProducerRecord<Integer, byte[]>> records = Seq.seq(this.topology.streamOutput()
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(Serdes.ByteArray()))
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::value)
                .anySatisfy(s3BackedText -> assertThat(s3BackedText).isNull());
    }

    private Properties createProperties(final Properties properties) {
        final AwsBasicCredentials credentials = this.getCredentials();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
        properties.put(AbstractLargeMessageConfig.S3_ENDPOINT_CONFIG, this.getEndpointOverride().toString());
        properties.put(AbstractLargeMessageConfig.S3_REGION_CONFIG, this.getRegion().id());
        properties.put(AbstractLargeMessageConfig.S3_ACCESS_KEY_CONFIG, credentials.accessKeyId());
        properties.put(AbstractLargeMessageConfig.S3_SECRET_KEY_CONFIG, credentials.secretAccessKey());
        properties.put(LargeMessageSerdeConfig.KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(LargeMessageSerdeConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        return properties;
    }

    private void expectBackedText(final String basePath, final String expected, final byte[] s3BackedText,
            final String type) {
        final BlobStorageURI uri = deserializeUriWithFlag(s3BackedText);
        this.expectBackedText(uri, basePath, type, expected);
    }

    private void expectBackedText(final String basePath, final String expected, final byte[] s3BackedText,
            final String type, final Headers headers, final boolean isKey) {
        final BlobStorageURI uri = deserializeUri(s3BackedText);
        this.expectBackedText(uri, basePath, type, expected);
        assertThat(headers.headers(getHeaderName(isKey))).hasSize(1);
    }

    private void expectBackedText(final BlobStorageURI uri, final String basePath, final String type,
            final String expected) {
        assertThat(uri).asString().startsWith(basePath + OUTPUT_TOPIC + "/" + type + "/");
        final byte[] bytes = this.readBytes(uri);
        final String deserialized = STRING_DESERIALIZER.deserialize(null, bytes);
        assertThat(deserialized).isEqualTo(expected);
    }

    private byte[] readBytes(final BlobStorageURI uri) {
        try (final InputStream objectContent = this.getS3Client().getObject(GetObjectRequest.builder()
                .bucket(uri.getBucket())
                .key(uri.getKey())
                .build())) {
            return IoUtils.toByteArray(objectContent);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void createTopology(final Function<? super Properties, ? extends Topology> topologyFactory,
            final Properties properties) {
        this.topology = new TestTopology<>(topologyFactory, this.createProperties(properties));
        this.topology.start();
    }

}
