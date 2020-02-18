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

import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import net.mguenther.kafka.junit.EmbeddedConnectConfig;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.SendKeyValues;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.file.FileStreamSinkConnector;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class S3BackedConverterIntegrationTest {
    public static final String BUCKET_NAME = "testbucket";
    public static final String S3_KEY_NAME = "contentkey";
    public static final String TOPIC = "input";
    public static final String EXTRACT_RECORD_KEY = "key1";
    public static final String DOWNLOAD_RECORD_KEY = "key2";

    @RegisterExtension
    static final S3MockExtension S3_MOCK = S3MockExtension.builder().silent()
            .withSecureConnection(false).build();

    private EmbeddedKafkaCluster kafkaCluster;
    private Path outputFile;

    @BeforeEach
    void setUp() throws IOException {
        this.outputFile = Files.createTempFile("test", "temp");
        S3_MOCK.createS3Client().createBucket(BUCKET_NAME);
        this.kafkaCluster = this.createCluster();
        this.kafkaCluster.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        this.kafkaCluster.stop();
        Files.deleteIfExists(this.outputFile);
    }

    @Test
    void shouldProcessRecordsCorrectly() throws InterruptedException, IOException {
        this.kafkaCluster
                .send(SendKeyValues.to(TOPIC, Collections.singletonList(new KeyValue<>(DOWNLOAD_RECORD_KEY, "toS3")))
                        .withAll(this.createProducerProperties(true)).build());

        this.kafkaCluster.send(SendKeyValues
                .to(TOPIC, Collections.singletonList(new KeyValue<>(EXTRACT_RECORD_KEY, "local")))
                .withAll(this.createProducerProperties(false)).build());

        // makes sure that both records are processed
        Thread.sleep(Duration.ofSeconds(2).toMillis());
        final List<String> output = Files.readAllLines(this.outputFile);
        assertThat(output).containsExactly("toS3", "local");
    }


    private EmbeddedKafkaCluster createCluster() {
        return EmbeddedKafkaCluster.provisionWith(EmbeddedKafkaClusterConfig
                .create()
                .provisionWith(
                        EmbeddedConnectConfig
                                .create()
                                .deployConnector(this.config())
                                .build()
                ).build());
    }

    private Properties config() {
        final Properties properties = new Properties();
        properties.put(ConnectorConfig.NAME_CONFIG, "test");
        properties.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "FileStreamSink");
        properties.put(SinkConnector.TOPICS_CONFIG, TOPIC);
        properties.put(FileStreamSinkConnector.FILE_CONFIG, this.outputFile.toString());
        properties.put("key.ignore", false);
        properties.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        properties.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, S3BackedConverter.class.getName());
        this.createS3BackedProperties().forEach((key, value) -> properties.put("value.converter." + key, value));
        return properties;
    }

    private Properties createProducerProperties(final boolean shouldBack) {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, S3BackedSerializer.class);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaCluster.getBrokerList());
        properties.put(S3BackedSerdeConfig.MAX_BYTE_SIZE_CONFIG, Integer.toString(shouldBack ? 0 : Integer.MAX_VALUE));
        properties.putAll(this.createS3BackedProperties());
        return properties;
    }

    private Properties createS3BackedProperties() {
        final Properties properties = new Properties();
        properties.put(S3BackedSerdeConfig.S3_ENDPOINT_CONFIG, "http://localhost:" + S3_MOCK.getHttpPort());
        properties.put(S3BackedSerdeConfig.S3_REGION_CONFIG, "us-east-1");
        properties.put(S3BackedSerdeConfig.S3_ACCESS_KEY_CONFIG, "foo");
        properties.put(S3BackedSerdeConfig.S3_SECRET_KEY_CONFIG, "bar");
        properties.put(S3BackedSerdeConfig.S3_ENABLE_PATH_STYLE_ACCESS_CONFIG, true);
        properties.put(S3BackedSerdeConfig.KEY_SERDE_CLASS_CONFIG, StringSerde.class.getName());
        properties.put(S3BackedSerdeConfig.VALUE_SERDE_CLASS_CONFIG, StringSerde.class.getName());
        properties.put(S3BackedSerdeConfig.BASE_PATH_CONFIG, String.format("s3://%s/%s", BUCKET_NAME, S3_KEY_NAME));
        properties.put(S3BackedConverterConfig.CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        return properties;
    }
}
