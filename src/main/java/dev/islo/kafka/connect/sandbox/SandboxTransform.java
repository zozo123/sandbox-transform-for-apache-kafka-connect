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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs user-supplied code against each record inside an isolated {@link Sandbox}.
 *
 * <p>Kafka Connect isolates plugins by classloader only (KIP-146), which separates dependencies
 * but grants every transform the worker's full privileges: an SMT shares a JVM with other
 * connectors and can read their configurations, open sockets, touch the filesystem, or call
 * {@code System.exit}. This transform moves the user's logic behind a real boundary while leaving
 * the rest of Connect untouched.
 *
 * <p>Configure it like any other SMT:
 * <pre>
 * transforms=redact
 * transforms.redact.type=dev.islo.kafka.connect.sandbox.SandboxTransform
 * transforms.redact.sandbox.provider=wasm
 * transforms.redact.sandbox.module=/opt/transforms/redact.wasm
 * </pre>
 *
 * <p>Errors are deliberately split in two so that Connect's existing machinery keeps working. A
 * record the guest rejects raises {@link DataException}, which {@code errors.tolerance} and the
 * dead-letter queue already handle. A broken boundary raises {@link SandboxException}, which
 * fails the task, because silently continuing without isolation would defeat the point.
 *
 * <p>Tombstones pass through untouched: there is no value to transform, and dropping them would
 * break compaction semantics for downstream consumers.
 */
public class SandboxTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(SandboxTransform.class);

    private final ReentrantLock lock = new ReentrantLock();

    private SandboxTransformConfig config;
    private RecordEnvelopeCodec codec;
    private volatile Sandbox runtime;

    @Override
    public ConfigDef config() {
        return SandboxTransformConfig.config();
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new SandboxTransformConfig(configs);
        this.codec = new RecordEnvelopeCodec(config.schemasEnable());
        final SandboxProvider provider = resolveProvider(config.providerName());
        this.runtime = provider.create(configs);
        log.info("Sandbox transform ready using the \"{}\" provider: {}",
            provider.name(), provider.capabilities());
    }

    @Override
    public R apply(final R record) {
        if (record.value() == null) {
            return record;
        }
        final Sandbox current = runtime;
        if (current == null) {
            throw new SandboxException("Sandbox transform used before configure() or after close()");
        }

        final byte[] request = codec.encode(record);

        // The Transformation contract requires apply() to be thread-safe, and a boundary is a
        // single conversation. Connect gives each task its own transform instance on its own
        // thread, so this lock is essentially uncontended in practice; it exists for correctness.
        final byte[] response;
        lock.lock();
        try {
            if (!current.isAlive()) {
                throw new SandboxException("Sandbox is no longer alive");
            }
            response = current.call(request);
        } finally {
            lock.unlock();
        }

        final GuestResponse guest = codec.decode(record.topic(), response);
        if (guest.isDrop()) {
            return null;
        }
        if (guest.isError()) {
            throw new DataException("Sandbox rejected the record: " + guest.error());
        }

        final SchemaAndValue value = guest.value();
        final String topic = guest.topic() != null ? guest.topic() : record.topic();
        return record.newRecord(
            topic,
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            value.schema(),
            value.value(),
            record.timestamp(),
            record.headers()
        );
    }

    @Override
    public void close() {
        final Sandbox current = runtime;
        runtime = null;
        if (current != null) {
            current.close();
        }
    }

    /**
     * Reports the plugin version.
     *
     * <p>Intentionally not annotated with {@code @Override}: Kafka 4.x added {@code version()} to
     * {@code Transformation} via {@code ConnectPlugin}, while 3.x has no such method. Declaring it
     * without the annotation overrides the newer default and remains a harmless extra method on
     * older workers, so one artifact serves both.
     */
    public String version() {
        final String implementation = getClass().getPackage().getImplementationVersion();
        return implementation != null ? implementation : "0.1.0-SNAPSHOT";
    }

    private static SandboxProvider resolveProvider(final String name) {
        // Load through this class's own classloader: under Connect that is the plugin
        // classloader, which is what actually contains the bundled runtimes.
        final ServiceLoader<SandboxProvider> loader =
            ServiceLoader.load(SandboxProvider.class, SandboxTransform.class.getClassLoader());
        final List<String> available = new ArrayList<>();
        for (final SandboxProvider factory : loader) {
            if (factory.name().equals(name)) {
                return factory;
            }
            available.add(factory.name());
        }
        throw new SandboxException(
            "No sandbox runtime named \"" + name + "\". Available runtimes: " + available);
    }
}
