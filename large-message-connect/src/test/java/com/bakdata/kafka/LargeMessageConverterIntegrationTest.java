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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.file.FileStreamSinkConnector;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

class LargeMessageConverterIntegrationTest extends AmazonS3IntegrationTest {
    private static final String BUCKET_NAME = "testbucket";
    private static final String S3_KEY_NAME = "contentkey";
    private static final String TOPIC = "input";
    private static final String EXTRACT_RECORD_KEY = "key1";
    private static final String DOWNLOAD_RECORD_KEY = "key2";
    private EmbeddedConnectCluster kafkaCluster;
    private Path outputFile;

    private static String asValueConfig(final String key) {
        return ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG + "." + key;
    }

    @BeforeEach
    void setUp() throws IOException {
        this.outputFile = Files.createTempFile("test", "temp");
        final S3Client s3 = this.getS3Client();
        s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
        this.kafkaCluster = new EmbeddedConnectCluster.Builder()
                .name("test-cluster")
                .workerProps(new HashMap<>(Map.of("plugin.discovery",
                        "hybrid_warn"))) // map needs to be mutable // FIXME make compatible with service discovery
                .build();
        this.kafkaCluster.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        this.kafkaCluster.stop();
        Files.deleteIfExists(this.outputFile);
    }

    @Test
    void shouldProcessRecordsCorrectly() throws InterruptedException, IOException {
        this.kafkaCluster.kafka().createTopic(TOPIC);
        this.kafkaCluster.configureConnector("test", this.config());
        try (final Producer<byte[], byte[]> producer = this.kafkaCluster.kafka()
                .createProducer(Collections.emptyMap())) {
            producer.send(this.createRecord(DOWNLOAD_RECORD_KEY, "toS3", true));
            producer.send(this.createRecord(EXTRACT_RECORD_KEY, "local", false));
        }

        // makes sure that both records are processed
        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        final List<String> output = Files.readAllLines(this.outputFile);
        assertThat(output).containsExactly("toS3", "local");
    }

    private ProducerRecord<byte[], byte[]> createRecord(final String key, final String value,
            final boolean shouldBack) {
        try (final Serializer<String> keySerializer = new StringSerializer();
                final Serializer<String> valueSerializer = this.createSerializer(shouldBack)) {
            final byte[] keyBytes = keySerializer.serialize(TOPIC, key);
            final byte[] valueBytes = valueSerializer.serialize(TOPIC, value);
            return new ProducerRecord<>(TOPIC, keyBytes, valueBytes);
        }
    }

    private Map<String, String> createS3BackedProperties() {
        final Map<String, String> properties = new HashMap<>(this.getLargeMessageConfig());
        properties.put(LargeMessageSerdeConfig.KEY_SERDE_CLASS_CONFIG, StringSerde.class.getName());
        properties.put(LargeMessageSerdeConfig.VALUE_SERDE_CLASS_CONFIG, StringSerde.class.getName());
        properties.put(
                AbstractLargeMessageConfig.BASE_PATH_CONFIG, String.format("s3://%s/%s", BUCKET_NAME, S3_KEY_NAME));
        return properties;
    }

    private Map<String, String> config() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, FileStreamSinkConnector.class.getName());
        properties.put(SinkConnector.TOPICS_CONFIG, TOPIC);
        properties.put(FileStreamSinkConnector.FILE_CONFIG, this.outputFile.toString());
        properties.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        properties.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, LargeMessageConverter.class.getName());
        properties.put(asValueConfig(LargeMessageConverterConfig.CONVERTER_CLASS_CONFIG),
                StringConverter.class.getName());
        this.createS3BackedProperties().forEach(
                (key, value) -> properties.put(asValueConfig(key), value));
        return properties;
    }

    private Serializer<String> createSerializer(final boolean shouldBack) {
        final Map<String, Object> properties = new HashMap<>();
        properties.put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG,
                Integer.toString(shouldBack ? 0 : Integer.MAX_VALUE));
        properties.putAll(this.createS3BackedProperties());
        final Serializer<String> serializer = new LargeMessageSerializer<>();
        serializer.configure(properties, false);
        return serializer;
    }
}
