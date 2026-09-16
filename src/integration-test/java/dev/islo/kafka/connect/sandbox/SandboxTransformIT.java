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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Runs the transform inside a real Connect worker, against a real Kafka broker, with a real
 * WebAssembly guest.
 *
 * <p>Everything the unit tests stub out is real here: the worker loads the transform from
 * {@code plugin.path} through Connect's own plugin classloader, a source connector produces
 * {@code Struct} records through the configured converters, and the transform chain runs inside
 * the worker's task thread. This is the only test that proves the plugin actually installs.
 */
class SandboxTransformIT {

    /**
     * Set {@code -Dintegration-test.bootstrap.servers} to run against a broker you started
     * yourself; otherwise one is started with Testcontainers. The override exists because
     * Testcontainers cannot always reach a Docker daemon from inside a build container, and
     * because being able to point this at any broker is useful in its own right.
     */
    private static final String EXTERNAL_BROKER =
        System.getProperty("integration-test.bootstrap.servers");

    private KafkaContainer kafka;
    private ConnectRunner connect;
    private String bootstrapServers;

    @BeforeEach
    void startWorker() {
        // Not an assumption. The integrationTest task always sets this, so an absent value means
        // the build is wired wrong -- and an aborted assumption here would abort the whole class
        // silently, leaving a green run that started no worker and asserted nothing.
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
    void masksCardNumbersInsideARealConnectWorker() throws Exception {
        final String module = System.getProperty("integration-test.wasm.module");
        assertThat(module).as("integration-test.wasm.module").isNotNull();

        // A missing guest is a failure, not a skip -- unless a human explicitly asked to skip by
        // running with -PallowMissingGuest, which is the escape hatch for a contributor with no
        // Rust toolchain. The distinction is the whole point: a skip nobody requested is how a
        // suite reports success while proving nothing.
        if (Boolean.getBoolean("integration-test.allow-missing-guest")) {
            assumeThat(new File(module)).as("built rust guest (skip requested)").exists();
        } else {
            assertThat(new File(module)).as("built rust guest").exists();
        }

        // A fresh topic per run: the consumer reads from earliest, so a reused broker could
        // otherwise serve records produced by an earlier run and mask a broken transform.
        final String topic = "payments-" + java.util.UUID.randomUUID();

        final Map<String, String> connector = new HashMap<>();
        connector.put("name", "payments-source-" + topic);
        connector.put(PaymentsSourceConnector.TOPIC_CONFIG, topic);
        connector.put("connector.class", PaymentsSourceConnector.class.getName());
        connector.put("tasks.max", "1");
        connector.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connector.put("value.converter.schemas.enable", "false");
        connector.put("transforms", "sandbox");
        connector.put("transforms.sandbox.type",
            "dev.islo.kafka.connect.sandbox.SandboxTransform");
        connector.put("transforms.sandbox.sandbox.provider", "wasm");
        connector.put("transforms.sandbox.sandbox.module", module);
        connector.put("transforms.sandbox.schemas.enable", "false");
        connect.createConnector(connector);

        final List<String> values = consume(topic, PaymentsSourceConnector.RECORDS - 1);

        // Four records survive; the fifth is flagged internal and the guest drops it.
        assertThat(values).hasSize(PaymentsSourceConnector.RECORDS - 1);
        assertThat(values).allSatisfy(value -> {
            assertThat(value).contains(PaymentsSourceConnector.MASKED);
            assertThat(value).doesNotContain(PaymentsSourceConnector.CARD);
        });
    }

    private List<String> consume(final String topic, final int expected) {
        final Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "sandbox-it-" + System.nanoTime());
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        final List<String> collected = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            final long deadline = System.currentTimeMillis() + 60_000;
            while (collected.size() < expected && System.currentTimeMillis() < deadline) {
                final ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(500));
                for (final ConsumerRecord<String, String> record : records) {
                    collected.add(record.value());
                }
            }
        }
        return collected;
    }
}
