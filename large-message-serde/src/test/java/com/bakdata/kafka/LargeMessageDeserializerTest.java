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

import static com.bakdata.kafka.HeaderLargeMessagePayloadProtocol.getHeaderName;
import static com.bakdata.kafka.LargeMessagePayload.ofBytes;
import static com.bakdata.kafka.LargeMessagePayload.ofUri;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.IntegerSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class LargeMessageDeserializerTest extends AmazonS3IntegrationTest {

    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";
    private static final LargeMessagePayloadProtocol HEADER_PROTOCOL = new HeaderLargeMessagePayloadProtocol();
    private static final LargeMessagePayloadProtocol BYTE_FLAG_PROTOCOL = new ByteFlagLargeMessagePayloadProtocol();
    private TestTopology<Integer, String> topology = null;

    private static byte[] serializeUri(final String uri) {
        return BYTE_FLAG_PROTOCOL.serialize(ofUri(uri), new RecordHeaders(), false);
    }

    private static byte[] serializeUri(final String uri, final Headers headers, final boolean isKey) {
        return HEADER_PROTOCOL.serialize(ofUri(uri), headers, isKey);
    }

    private static byte[] serialize(final byte[] bytes) {
        return BYTE_FLAG_PROTOCOL.serialize(ofBytes(bytes), new RecordHeaders(), false);
    }

    private static byte[] serialize(final byte[] bytes, final Headers headers, final boolean isKey) {
        return HEADER_PROTOCOL.serialize(ofBytes(bytes), headers, isKey);
    }

    private static Topology createKeyTopology(final Map<String, Object> properties) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Serde<String> serde = new LargeMessageSerde<>();
        serde.configure(new StreamsConfig(properties).originals(), true);
        final KStream<String, Integer> input = builder.stream(INPUT_TOPIC, Consumed.with(serde, Serdes.Integer()));
        input.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Integer()));
        return builder.build();
    }

    private static Topology createValueTopology(final Map<String, Object> properties) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Serde<String> serde = new LargeMessageSerde<>();
        serde.configure(new StreamsConfig(properties).originals(), false);
        final KStream<Integer, String> input = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.Integer(), serde));
        input.to(OUTPUT_TOPIC, Produced.with(Serdes.Integer(), Serdes.String()));
        return builder.build();
    }

    private static Topology createKeyAndValueTopology(final Map<String, Object> properties) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Serde<String> keySerde = new LargeMessageSerde<>();
        keySerde.configure(new StreamsConfig(properties).originals(), true);
        final Serde<String> valueSerde = new LargeMessageSerde<>();
        valueSerde.configure(new StreamsConfig(properties).originals(), false);
        final KStream<String, String> input = builder.stream(INPUT_TOPIC, Consumed.with(keySerde, valueSerde));
        input.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    private static byte[] createNonBackedText(final String text) {
        return serialize(serialize(text));
    }

    private static byte[] createNonBackedText(final String text, final Headers headers, final boolean isKey) {
        return serialize(serialize(text), headers, isKey);
    }

    private static byte[] serialize(final String text) {
        return Serdes.String().serializer().serialize(null, text);
    }

    private static byte[] createBackedText(final String bucket, final String key) {
        final String uri = "s3://" + bucket + "/" + key;
        return serializeUri(uri);
    }

    private static byte[] createBackedText(final String bucket, final String key, final Headers headers,
            final boolean isKey) {
        final String uri = "s3://" + bucket + "/" + key;
        return serializeUri(uri, headers, isKey);
    }

    @AfterEach
    void tearDown() {
        if (this.topology != null) {
            this.topology.stop();
        }
    }

    @Test
    void shouldReadNonBackedTextValue() {
        this.createTopology(LargeMessageDeserializerTest::createValueTopology);
        this.topology.input()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.ByteArray())
                .add(1, createNonBackedText("foo"));
        final List<ProducerRecord<Integer, String>> records = this.topology.streamOutput()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.String())
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::value)
                .containsExactlyInAnyOrder("foo");
    }

    @Test
    void shouldReadNonBackedTextValueWithHeaders() {
        this.createTopology(LargeMessageDeserializerTest::createValueTopology);
        final Headers headers = new RecordHeaders();
        this.topology.input()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.ByteArray())
                .add(1, createNonBackedText("foo", headers, false), headers);
        final List<ProducerRecord<Integer, String>> records = this.topology.streamOutput()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.String())
                .toList();
        assertThat(records)
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    assertThat(producerRecord.value()).isEqualTo("foo");
                    assertThat(producerRecord.headers()).isEmpty();
                });
    }

    @Test
    void shouldReadNullValue() {
        this.createTopology(LargeMessageDeserializerTest::createValueTopology);
        this.topology.input()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.ByteArray())
                .add(1, null);
        final List<ProducerRecord<Integer, String>> records = this.topology.streamOutput()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.String())
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::value)
                .containsExactlyInAnyOrder(new String[]{null});
    }

    @Test
    void shouldReadNonBackedTextKey() {
        this.createTopology(LargeMessageDeserializerTest::createKeyTopology);
        this.topology.input()
                .withKeySerde(Serdes.ByteArray())
                .withValueSerde(Serdes.Integer())
                .add(createNonBackedText("foo"), 1);
        final List<ProducerRecord<String, Integer>> records = this.topology.streamOutput()
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Integer())
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::key)
                .containsExactlyInAnyOrder("foo");
    }

    @Test
    void shouldReadNonBackedTextKeyWithHeaders() {
        this.createTopology(LargeMessageDeserializerTest::createKeyTopology);
        final Headers headers = new RecordHeaders();
        this.topology.input()
                .withKeySerde(Serdes.ByteArray())
                .withValueSerde(Serdes.Integer())
                .add(createNonBackedText("foo", headers, true), 1, headers);
        final List<ProducerRecord<String, Integer>> records = this.topology.streamOutput()
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Integer())
                .toList();
        assertThat(records)
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    assertThat(producerRecord.key()).isEqualTo("foo");
                    assertThat(producerRecord.headers()).isEmpty();
                });
    }

    @Test
    void shouldReadNullKey() {
        this.createTopology(LargeMessageDeserializerTest::createKeyTopology);
        this.topology.input()
                .withKeySerde(Serdes.ByteArray())
                .withValueSerde(Serdes.Integer())
                .add(null, 1);
        final List<ProducerRecord<String, Integer>> records = this.topology.streamOutput()
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Integer())
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::key)
                .containsExactlyInAnyOrder(new String[]{null});
    }

    @Test
    void shouldReadBackedTextValue() {
        final String bucket = "bucket";
        this.getS3Client().createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        final String key = "key";
        this.store(bucket, key, "foo");
        this.createTopology(LargeMessageDeserializerTest::createValueTopology);
        this.topology.input()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.ByteArray())
                .add(1, createBackedText(bucket, key));
        final List<ProducerRecord<Integer, String>> records = this.topology.streamOutput()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.String())
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::value)
                .containsExactlyInAnyOrder("foo");
    }

    @Test
    void shouldReadBackedTextValueWithHeaders() {
        final String bucket = "bucket";
        this.getS3Client().createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        final String key = "key";
        this.store(bucket, key, "foo");
        this.createTopology(LargeMessageDeserializerTest::createValueTopology);
        final Headers headers = new RecordHeaders();
        final byte[] value = createBackedText(bucket, key, headers, false);
        // add compression header for 'none' type, so we can assert it is also properly removed
        headers.add(CompressionType.HEADER_NAME, new byte[]{CompressionType.NONE.getId()});
        this.topology.input()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.ByteArray())
                .add(1, value, headers);
        final List<ProducerRecord<Integer, String>> records = this.topology.streamOutput()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.String())
                .toList();
        assertThat(records)
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    assertThat(producerRecord.value()).isEqualTo("foo");
                    assertThat(producerRecord.headers()).isEmpty();
                });
    }

    @Test
    void shouldReadBackedTextKey() {
        final String bucket = "bucket";
        this.getS3Client().createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        final String key = "key";
        this.store(bucket, key, "foo");
        this.createTopology(LargeMessageDeserializerTest::createKeyTopology);
        this.topology.input()
                .withKeySerde(Serdes.ByteArray())
                .withValueSerde(Serdes.Integer())
                .add(createBackedText(bucket, key), 1);
        final List<ProducerRecord<String, Integer>> records = this.topology.streamOutput()
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Integer())
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::key)
                .containsExactlyInAnyOrder("foo");
    }

    @Test
    void shouldReadBackedTextKeyWithHeaders() {
        final String bucket = "bucket";
        this.getS3Client().createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        final String key = "key";
        this.store(bucket, key, "foo");
        this.createTopology(LargeMessageDeserializerTest::createKeyTopology);
        final Headers headers = new RecordHeaders();
        this.topology.input()
                .withKeySerde(Serdes.ByteArray())
                .withValueSerde(Serdes.Integer())
                .add(createBackedText(bucket, key, headers, true), 1, headers);
        final List<ProducerRecord<String, Integer>> records = this.topology.streamOutput()
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Integer())
                .toList();
        assertThat(records)
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    assertThat(producerRecord.key()).isEqualTo("foo");
                    assertThat(producerRecord.headers()).isEmpty();
                });
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldRetainBackedHeadersOnSerializationException(final boolean isKey) {
        final String bucket = "bucket";
        this.getS3Client().createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        this.assertCorrectSerializationExceptionBehavior(isKey, (content, headers, _isKey) -> {
            final String key = "key";
            this.store(bucket, key, content);
            return createBackedText(bucket, key, headers, _isKey);
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldRetainNonBackedHeadersOnSerializationException(final boolean isKey) {
        this.assertCorrectSerializationExceptionBehavior(isKey, LargeMessageDeserializerTest::createNonBackedText);
    }

    @Test
    void shouldReadNonBackedTextKeyAndBackedValueWithHeaders() {
        final String bucket = "bucket";
        this.getS3Client().createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        final String key = "key";
        this.store(bucket, key, "bar");
        this.createTopology(LargeMessageDeserializerTest::createKeyAndValueTopology);
        final Headers headers = new RecordHeaders();
        this.topology.input()
                .withKeySerde(Serdes.ByteArray())
                .withValueSerde(Serdes.ByteArray())
                .add(createNonBackedText("foo", headers, true), createBackedText(bucket, key, headers, false), headers);
        final List<ProducerRecord<String, String>> records = this.topology.streamOutput()
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
                .toList();
        assertThat(records)
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    assertThat(producerRecord.key()).isEqualTo("foo");
                    assertThat(producerRecord.value()).isEqualTo("bar");
                    assertThat(producerRecord.headers()).isEmpty();
                });
    }

    @Test
    void shouldReadBackedTextKeyAndNonBackedValueWithHeaders() {
        final String bucket = "bucket";
        this.getS3Client().createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        final String key = "key";
        this.store(bucket, key, "foo");
        this.createTopology(LargeMessageDeserializerTest::createKeyAndValueTopology);
        final Headers headers = new RecordHeaders();
        this.topology.input()
                .withKeySerde(Serdes.ByteArray())
                .withValueSerde(Serdes.ByteArray())
                .add(createBackedText(bucket, key, headers, true), createNonBackedText("bar", headers, false), headers);
        final List<ProducerRecord<String, String>> records = this.topology.streamOutput()
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
                .toList();
        assertThat(records)
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    assertThat(producerRecord.key()).isEqualTo("foo");
                    assertThat(producerRecord.value()).isEqualTo("bar");
                    assertThat(producerRecord.headers()).isEmpty();
                });
    }

    private Map<String, Object> createProperties() {
        final Map<String, String> endpointConfig = this.getLargeMessageConfig();
        final Map<String, Object> properties = new HashMap<>();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
        properties.putAll(endpointConfig);
        properties.put(LargeMessageSerdeConfig.KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(LargeMessageSerdeConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        return properties;
    }

    private void store(final String bucket, final String key, final String s) {
        final PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        this.getS3Client().putObject(request, RequestBody.fromBytes(s.getBytes()));
    }

    private void assertCorrectSerializationExceptionBehavior(final boolean isKey,
            final MessageFactory messageFactory) {
        try (final Deserializer<String> deserializer = new LargeMessageDeserializer<>()) {
            final Headers headers = new RecordHeaders();
            final Map<String, Object> config = new HashMap<>(this.getLargeMessageConfig());
            config.put(isKey ? LargeMessageSerdeConfig.KEY_SERDE_CLASS_CONFIG
                    : LargeMessageSerdeConfig.VALUE_SERDE_CLASS_CONFIG, IntegerSerde.class);
            deserializer.configure(config, isKey);
            final byte[] message = messageFactory.apply("foo", headers, isKey);
            assertThatThrownBy(() -> deserializer.deserialize(null, headers, message))
                    .isInstanceOf(SerializationException.class)
                    .hasMessage("Size of data received by IntegerDeserializer is not 4");
            assertThat(headers.headers(getHeaderName(isKey))).hasSize(1);
        }
    }

    private void createTopology(final Function<? super Map<String, Object>, ? extends Topology> topologyFactory) {
        this.topology = new TestTopology<>(topologyFactory, this.createProperties());
        this.topology.start();
    }

    @FunctionalInterface
    private interface MessageFactory {
        byte[] apply(String content, Headers headers, boolean isKey);
    }

}
