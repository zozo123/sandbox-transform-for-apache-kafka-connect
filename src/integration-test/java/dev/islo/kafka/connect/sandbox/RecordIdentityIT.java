/*
 * Copyright 2026 Yossi Eliaz
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.islo.kafka.connect.sandbox;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that masking a record's value leaves the rest of the record alone.
 *
 * <p>{@link SandboxTransform#apply} rebuilds every record it transforms, carrying the key, the key
 * schema, the partition, the timestamp and the headers across by hand. Nothing asserted any of
 * that. A regression that dropped headers -- or replaced the key with null -- would have passed
 * the entire suite, because every other assertion in it looks only at the value.
 *
 * <p>That is not a hypothetical class of bug for this plugin in particular. The key is what log
 * compaction and partitioning are computed from, so losing it silently re-partitions a topic and
 * breaks compaction for every downstream consumer; headers are where trace context, lineage and
 * schema identity travel. Both survive a redaction transform or the transform is not safe to
 * deploy, however correct the masking is.
 */
class RecordIdentityIT {

    private static final String EXTERNAL_BROKER =
        System.getProperty("integration-test.bootstrap.servers");

    private KafkaContainer kafka;
    private ConnectRunner connect;
    private String bootstrapServers;

    @BeforeEach
    void startWorker() {
        final String pluginDir = System.getProperty("integration-test.plugin.dir");
        assertThat(pluginDir)
            .as("integration-test.plugin.dir, set by the integrationTest task")
            .isNotNull();

        if (EXTERNAL_BROKER != null && !EXTERNAL_BROKER.isEmpty()) {
            bootstrapServers = EXTERNAL_BROKER;
        } else {
            kafka = new KafkaContainer("apache/kafka:3.8.1");
            kafka.start();
            bootstrapServers = kafka.getBootstrapServers();
        }

        connect = new ConnectRunner(new File(pluginDir), bootstrapServers);
        connect.start();
    }

    @AfterEach
    void stopWorker() {
        if (connect != null) {
            connect.stop();
        }
        if (kafka != null) {
            kafka.stop();
        }
    }

    @Test
    void theKeyAndHeadersSurviveTheTransform() throws Exception {
        final String module = System.getProperty("integration-test.wasm.module");
        assertThat(module).as("integration-test.wasm.module").isNotNull();
        assertThat(new File(module)).as("built rust guest").exists();

        final String topic = "identity-" + UUID.randomUUID();

        final Map<String, String> connector = new HashMap<>();
        connector.put("name", "identity-source-" + topic);
        connector.put(PaymentsSourceConnector.TOPIC_CONFIG, topic);
        connector.put("connector.class", PaymentsSourceConnector.class.getName());
        connector.put("tasks.max", "1");
        connector.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connector.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connector.put("value.converter.schemas.enable", "false");
        connector.put("transforms", "sandbox");
        connector.put("transforms.sandbox.type",
            "dev.islo.kafka.connect.sandbox.SandboxTransform");
        connector.put("transforms.sandbox.sandbox.provider", "wasm");
        connector.put("transforms.sandbox.sandbox.module", module);
        connector.put("transforms.sandbox.schemas.enable", "false");
        connect.createConnector(connector);

        final int surviving = PaymentsSourceConnector.RECORDS - 1;
        final List<ConsumerRecord<String, String>> records = consume(topic, surviving);

        assertThat(records).hasSize(surviving);

        // Look records up by key rather than by position. Keyed records are hash-partitioned and
        // the consumer collects in poll order across partitions, so an index-based assertion here
        // would be relying on a single-partition topic without saying so.
        for (int i = 0; i < surviving; i++) {
            final String expectedKey = PaymentsSourceConnector.KEY_PREFIX + i;

            assertThat(records)
                .filteredOn(record -> expectedKey.equals(record.key()))
                .as("record with key %s", expectedKey)
                .singleElement()
                .satisfies(record -> {
                    // The value really was transformed -- without this the identity assertions
                    // below would hold just as well for a transform that did nothing at all.
                    assertThat(record.value()).contains(PaymentsSourceConnector.MASKED);
                    assertThat(record.value()).doesNotContain(PaymentsSourceConnector.CARD);

                    assertThat(headerValue(record, PaymentsSourceConnector.TRACE_HEADER))
                        .as("trace header on %s", expectedKey)
                        .isEqualTo("trace-" + indexOf(expectedKey));
                    assertThat(headerValue(record, PaymentsSourceConnector.ORIGIN_HEADER))
                        .as("origin header on %s", expectedKey)
                        .isEqualTo(PaymentsSourceConnector.ORIGIN_VALUE);
                });
        }

        // The dropped record takes its key with it: filtering a record out must not leave a
        // tombstone-shaped hole behind.
        final String droppedKey =
            PaymentsSourceConnector.KEY_PREFIX + (PaymentsSourceConnector.RECORDS - 1);
        assertThat(records).noneSatisfy(record ->
            assertThat(record.key()).isEqualTo(droppedKey));
    }

    private static int indexOf(final String key) {
        return Integer.parseInt(key.substring(PaymentsSourceConnector.KEY_PREFIX.length()));
    }

    private static String headerValue(final ConsumerRecord<String, String> record,
                                      final String name) {
        final Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private List<ConsumerRecord<String, String>> consume(final String topic, final int expected) {
        final Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "identity-it-" + System.nanoTime());
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        final List<ConsumerRecord<String, String>> collected = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            final long deadline = System.currentTimeMillis() + 60_000;
            while (collected.size() < expected && System.currentTimeMillis() < deadline) {
                final ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
                for (final ConsumerRecord<String, String> record : polled) {
                    collected.add(record);
                }
            }
        }
        return collected;
    }
}
