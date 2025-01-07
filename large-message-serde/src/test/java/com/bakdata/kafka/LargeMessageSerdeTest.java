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

import com.bakdata.fluent_kafka_streams_tests.TestInput;
import com.bakdata.fluent_kafka_streams_tests.TestOutput;
import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LargeMessageSerdeTest extends AmazonS3IntegrationTest {

    private static final String INPUT_TOPIC_1 = "input1";
    private static final String INPUT_TOPIC_2 = "input2";
    private static final String OUTPUT_TOPIC = "output";
    private TestTopology<Integer, String> topology;

    private static Topology createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, String> input1 =
                builder.stream(INPUT_TOPIC_1, Consumed.with(Serdes.String(), Serdes.String()))
                        .selectKey((k, v) -> k.substring(0, 1))
                        .toTable();
        final KTable<String, String> input2 =
                builder.stream(INPUT_TOPIC_2, Consumed.with(Serdes.String(), Serdes.String()))
                        .selectKey((k, v) -> k.substring(0, 1))
                        .toTable();
        final KTable<String, String> joined = input1.join(input2, (l, r) -> l + r);
        joined.toStream()
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    @BeforeEach
    void setup() {
        this.topology = new TestTopology<>(LargeMessageSerdeTest::createTopology, this.createLargeMessageProperties());
        this.topology.start();
    }

    @AfterEach
    void tearDown() {
        if (this.topology != null) {
            this.topology.stop();
        }
    }

    @Test
    void shouldJoin() {
        // this test creates a topology with a changelog store. The changelog store uses the Serde without headers
        this.getInput(INPUT_TOPIC_1)
                .add("a", "foo");
        this.getInput(INPUT_TOPIC_2)
                .add("a", "bar");
        final List<ProducerRecord<String, String>> records = this.getOutput().toList();
        assertThat(records)
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    assertThat(producerRecord.key()).isEqualTo("a");
                    assertThat(producerRecord.value()).isEqualTo("foobar");
                });
    }

    private Map<String, Object> createLargeMessageProperties() {
        final Map<String, String> endpointConfig = this.getLargeMessageConfig();
        final Map<String, Object> properties = new HashMap<>();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, LargeMessageSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, LargeMessageSerde.class);
        properties.putAll(endpointConfig);
        properties.put(LargeMessageSerdeConfig.KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(LargeMessageSerdeConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(AbstractLargeMessageConfig.USE_HEADERS_CONFIG, true);
        return properties;
    }

    private TestOutput<String, String> getOutput() {
        return this.topology.streamOutput()
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String());
    }

    private TestInput<String, String> getInput(final String topic) {
        return this.topology.input(topic)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String());
    }

}
