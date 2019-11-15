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

import static com.bakdata.kafka.S3BackedSerializer.serialize;
import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import org.apache.kafka.clients.producer.ProducerRecord;
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

class S3BackedDeserializerTest {

    @RegisterExtension
    static final S3MockExtension S3_MOCK = S3MockExtension.builder().silent()
            .withSecureConnection(false).build();
    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";
    private TestTopology<Integer, String> topology = null;

    private static Properties createProperties() {
        final Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        properties.setProperty(S3BackedSerdeConfig.S3_ENDPOINT_CONFIG, "http://localhost:" + S3_MOCK.getHttpPort());
        properties.setProperty(S3BackedSerdeConfig.S3_REGION_CONFIG, "us-east-1");
        properties.setProperty(S3BackedSerdeConfig.S3_ACCESS_KEY_CONFIG, "foo");
        properties.setProperty(S3BackedSerdeConfig.S3_SECRET_KEY_CONFIG, "bar");
        properties.put(S3BackedSerdeConfig.S3_ENABLE_PATH_STYLE_ACCESS_CONFIG, true);
        properties.put(S3BackedSerdeConfig.KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(S3BackedSerdeConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        return properties;
    }

    private static Topology createKeyTopology(final Properties properties) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Serde<String> serde = new S3BackedSerde<>();
        serde.configure(new StreamsConfig(properties).originals(), true);
        final KStream<String, Integer> input = builder.stream(INPUT_TOPIC, Consumed.with(serde, Serdes.Integer()));
        input.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Integer()));
        return builder.build();
    }

    private static Topology createValueTopology(final Properties properties) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Serde<String> serde = new S3BackedSerde<>();
        serde.configure(new StreamsConfig(properties).originals(), false);
        final KStream<Integer, String> input = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.Integer(), serde));
        input.to(OUTPUT_TOPIC, Produced.with(Serdes.Integer(), Serdes.String()));
        return builder.build();
    }

    private static void store(final String bucket, final String key, final String s) {
        S3_MOCK.createS3Client().putObject(bucket, key, new ByteArrayInputStream(s.getBytes()), new ObjectMetadata());
    }

    private static byte[] createNonBackedText(final String text) {
        return serialize(Serdes.String().serializer().serialize(null, text));
    }

    private static byte[] createBackedText(final String bucket, final String key) {
        final String uri = "s3://" + bucket + "/" + key;
        return serialize(uri);
    }

    private void createTopology(final Function<? super Properties, ? extends Topology> topologyFactory) {
        this.topology = new TestTopology<>(topologyFactory, createProperties());
        this.topology.start();
    }

    @AfterEach
    void tearDown() {
        if (this.topology != null) {
            this.topology.stop();
        }
    }

    @Test
    void shouldReadNonBackedTextValue() {
        this.createTopology(S3BackedDeserializerTest::createValueTopology);
        this.topology.input()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.ByteArray())
                .add(1, createNonBackedText("foo"));
        final List<ProducerRecord<Integer, String>> records = Seq.seq(this.topology.streamOutput()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.String()))
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::value)
                .containsExactlyInAnyOrder("foo");
    }

    @Test
    void shouldReadNonBackedTextKey() {
        this.createTopology(S3BackedDeserializerTest::createKeyTopology);
        this.topology.input()
                .withKeySerde(Serdes.ByteArray())
                .withValueSerde(Serdes.Integer())
                .add(createNonBackedText("foo"), 1);
        final List<ProducerRecord<String, Integer>> records = Seq.seq(this.topology.streamOutput()
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Integer()))
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::key)
                .containsExactlyInAnyOrder("foo");
    }

    @Test
    void shouldReadBackedTextValue() {
        final String bucket = "bucket";
        S3_MOCK.createS3Client().createBucket(bucket);
        final String key = "key";
        store(bucket, key, "foo");
        this.createTopology(S3BackedDeserializerTest::createValueTopology);
        this.topology.input()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.ByteArray())
                .add(1, createBackedText(bucket, key));
        final List<ProducerRecord<Integer, String>> records = Seq.seq(this.topology.streamOutput()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.String()))
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::value)
                .containsExactlyInAnyOrder("foo");
    }

    @Test
    void shouldReadBackedTextKey() {
        final String bucket = "bucket";
        S3_MOCK.createS3Client().createBucket(bucket);
        final String key = "key";
        store(bucket, key, "foo");
        this.createTopology(S3BackedDeserializerTest::createKeyTopology);
        this.topology.input()
                .withKeySerde(Serdes.ByteArray())
                .withValueSerde(Serdes.Integer())
                .add(createBackedText(bucket, key), 1);
        final List<ProducerRecord<String, Integer>> records = Seq.seq(this.topology.streamOutput()
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Integer()))
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::key)
                .containsExactlyInAnyOrder("foo");
    }

}