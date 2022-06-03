/*
 * MIT License
 *
 * Copyright (c) 2022 bakdata
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

import static net.mguenther.kafka.junit.Wait.delay;
import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
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
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.apache.ApacheSdkHttpService;

class LargeMessageConverterIntegrationTest {
    @RegisterExtension
    static final S3MockExtension S3_MOCK = S3MockExtension.builder().silent()
            .withSecureConnection(false).build();
    private static final String BUCKET_NAME = "testbucket";
    private static final String S3_KEY_NAME = "contentkey";
    private static final String TOPIC = "input";
    private static final String EXTRACT_RECORD_KEY = "key1";
    private static final String DOWNLOAD_RECORD_KEY = "key2";
    private EmbeddedKafkaCluster kafkaCluster;
    private Path outputFile;

    private static Properties createS3BackedProperties() {
        final Properties properties = new Properties();
        properties.put(AbstractLargeMessageConfig.S3_ENDPOINT_CONFIG, "http://localhost:" + S3_MOCK.getHttpPort());
        properties.put(AbstractLargeMessageConfig.S3_REGION_CONFIG, "us-east-1");
        properties.put(AbstractLargeMessageConfig.S3_ACCESS_KEY_CONFIG, "foo");
        properties.put(AbstractLargeMessageConfig.S3_SECRET_KEY_CONFIG, "bar");
        properties.put(LargeMessageSerdeConfig.KEY_SERDE_CLASS_CONFIG, StringSerde.class.getName());
        properties.put(LargeMessageSerdeConfig.VALUE_SERDE_CLASS_CONFIG, StringSerde.class.getName());
        properties.put(
                AbstractLargeMessageConfig.BASE_PATH_CONFIG, String.format("s3://%s/%s", BUCKET_NAME, S3_KEY_NAME));
        properties.put(LargeMessageConverterConfig.CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        return properties;
    }

    @BeforeEach
    void setUp() throws IOException {
        this.outputFile = Files.createTempFile("test", "temp");
        S3_MOCK.createS3Client().createBucket(BUCKET_NAME);
        System.setProperty(SdkSystemSetting.SYNC_HTTP_SERVICE_IMPL.property(), ApacheSdkHttpService.class.getName());
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
        delay(2, TimeUnit.SECONDS);
        final List<String> output = Files.readAllLines(this.outputFile);
        assertThat(output).containsExactly("toS3", "local");
    }

    private EmbeddedKafkaCluster createCluster() {
        return EmbeddedKafkaCluster.provisionWith(EmbeddedKafkaClusterConfig
                .newClusterConfig()
                .configure(
                        EmbeddedConnectConfig
                                .kafkaConnect()
                                .deployConnector(this.config())
                                .build())
                .build());
    }

    private Properties config() {
        final Properties properties = new Properties();
        properties.put(ConnectorConfig.NAME_CONFIG, "test");
        properties.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "FileStreamSink");
        properties.put(SinkConnector.TOPICS_CONFIG, TOPIC);
        properties.put(FileStreamSinkConnector.FILE_CONFIG, this.outputFile.toString());
        properties.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        properties.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, LargeMessageConverter.class.getName());
        createS3BackedProperties().forEach(
                (key, value) -> properties.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG + "." + key, value));
        return properties;
    }

    private Properties createProducerProperties(final boolean shouldBack) {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LargeMessageSerializer.class);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaCluster.getBrokerList());
        properties.put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG,
                Integer.toString(shouldBack ? 0 : Integer.MAX_VALUE));
        properties.putAll(createS3BackedProperties());
        return properties;
    }
}
