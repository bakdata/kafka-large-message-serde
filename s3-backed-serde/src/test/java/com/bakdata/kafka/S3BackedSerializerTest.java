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

import static com.bakdata.kafka.S3BackedRetrievingClient.deserializeUri;
import static com.bakdata.kafka.S3BackedRetrievingClient.getBytes;
import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.IOUtils;
import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import org.junit.jupiter.api.extension.RegisterExtension;

class S3BackedSerializerTest {

    @RegisterExtension
    static final S3MockExtension S3_MOCK = S3MockExtension.builder().silent()
            .withSecureConnection(false).build();
    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";
    private static final Deserializer<String> STRING_DESERIALIZER = Serdes.String().deserializer();
    private TestTopology<Integer, String> topology = null;

    private static Properties createProperties(final Properties properties) {
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        properties.setProperty(AbstractS3BackedConfig.S3_ENDPOINT_CONFIG, "http://localhost:" + S3_MOCK.getHttpPort());
        properties.setProperty(AbstractS3BackedConfig.S3_REGION_CONFIG, "us-east-1");
        properties.setProperty(AbstractS3BackedConfig.S3_ACCESS_KEY_CONFIG, "foo");
        properties.setProperty(AbstractS3BackedConfig.S3_SECRET_KEY_CONFIG, "bar");
        properties.put(AbstractS3BackedConfig.S3_ENABLE_PATH_STYLE_ACCESS_CONFIG, true);
        properties.put(S3BackedSerdeConfig.KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(S3BackedSerdeConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        return properties;
    }

    private static Topology createValueTopology(final Properties properties) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Map<String, Object> configs = new StreamsConfig(properties).originals();
        final Serde<String> serde = new S3BackedSerde<>();
        serde.configure(configs, false);
        final KStream<Integer, String> input =
                builder.stream(INPUT_TOPIC, Consumed.with(Serdes.Integer(), Serdes.String()));
        input.to(OUTPUT_TOPIC, Produced.with(Serdes.Integer(), serde));
        return builder.build();
    }

    private static Topology createKeyTopology(final Properties properties) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Map<String, Object> configs = new StreamsConfig(properties).originals();
        final Serde<String> serde = new S3BackedSerde<>();
        serde.configure(configs, true);
        final KStream<String, Integer> input =
                builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.Integer()));
        input.to(OUTPUT_TOPIC, Produced.with(serde, Serdes.Integer()));
        return builder.build();
    }

    private static void expectBackedText(final String basePath, final String expected, final byte[] s3BackedText,
            final String type) {
        final String uri = deserializeUri(s3BackedText);
        assertThat(uri).startsWith(basePath + OUTPUT_TOPIC + "/" + type + "/");
        final AmazonS3URI amazonS3URI = new AmazonS3URI(uri);
        final byte[] bytes = readBytes(amazonS3URI);
        final String deserialized = STRING_DESERIALIZER
                .deserialize(null, bytes);
        assertThat(deserialized).isEqualTo(expected);
    }

    private static byte[] readBytes(final AmazonS3URI amazonS3URI) {
        try (final S3Object object = S3_MOCK.createS3Client().getObject(amazonS3URI.getBucket(), amazonS3URI.getKey());
                final S3ObjectInputStream objectContent = object.getObjectContent()) {
            return IOUtils.toByteArray(objectContent);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void expectNonBackedText(final String expected, final byte[] s3BackedText) {
        assertThat(STRING_DESERIALIZER.deserialize(null, getBytes(s3BackedText)))
                .isInstanceOf(String.class)
                .isEqualTo(expected);
    }

    private void createTopology(final Function<? super Properties, ? extends Topology> topologyFactory,
            final Properties properties) {
        this.topology = new TestTopology<>(topologyFactory, createProperties(properties));
        this.topology.start();
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
        properties.put(AbstractS3BackedConfig.MAX_BYTE_SIZE_CONFIG, Integer.MAX_VALUE);
        this.createTopology(S3BackedSerializerTest::createKeyTopology, properties);
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
    void shouldWriteNonBackedNullKey() {
        final Properties properties = new Properties();
        properties.put(AbstractS3BackedConfig.MAX_BYTE_SIZE_CONFIG, Integer.MAX_VALUE);
        this.createTopology(S3BackedSerializerTest::createKeyTopology, properties);
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
        properties.put(AbstractS3BackedConfig.MAX_BYTE_SIZE_CONFIG, Integer.MAX_VALUE);
        this.createTopology(S3BackedSerializerTest::createValueTopology, properties);
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
    void shouldWriteNonBackedNullValue() {
        final Properties properties = new Properties();
        properties.put(AbstractS3BackedConfig.MAX_BYTE_SIZE_CONFIG, Integer.MAX_VALUE);
        this.createTopology(S3BackedSerializerTest::createValueTopology, properties);
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
        properties.put(AbstractS3BackedConfig.MAX_BYTE_SIZE_CONFIG, 0);
        properties.setProperty(AbstractS3BackedConfig.BASE_PATH_CONFIG, basePath);
        this.createTopology(S3BackedSerializerTest::createKeyTopology, properties);
        final AmazonS3 s3Client = S3_MOCK.createS3Client();
        s3Client.createBucket(bucket);
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
        s3Client.deleteBucket(bucket);
    }

    @Test
    void shouldWriteBackedNullKey() {
        final Properties properties = new Properties();
        properties.put(AbstractS3BackedConfig.MAX_BYTE_SIZE_CONFIG, 0);
        this.createTopology(S3BackedSerializerTest::createKeyTopology, properties);
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
        properties.put(AbstractS3BackedConfig.MAX_BYTE_SIZE_CONFIG, 0);
        properties.setProperty(AbstractS3BackedConfig.BASE_PATH_CONFIG, basePath);
        this.createTopology(S3BackedSerializerTest::createValueTopology, properties);
        final AmazonS3 s3Client = S3_MOCK.createS3Client();
        s3Client.createBucket(bucket);
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
        s3Client.deleteBucket(bucket);
    }

    @Test
    void shouldWriteBackedNullValue() {
        final Properties properties = new Properties();
        properties.put(AbstractS3BackedConfig.MAX_BYTE_SIZE_CONFIG, 0);
        this.createTopology(S3BackedSerializerTest::createValueTopology, properties);
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

}