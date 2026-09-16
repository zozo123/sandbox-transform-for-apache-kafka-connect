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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the real Rust guest in {@code examples/redact-rs}, not a hand-written test module.
 *
 * <p>Skipped unless the module has been built, so the suite still runs without a Rust toolchain:
 * <pre>
 * cd examples/redact-rs &amp;&amp; cargo build --release --target wasm32-unknown-unknown
 * </pre>
 */
class RustGuestIT {

    private static final Path MODULE =
        Paths.get("examples/redact-rs/target/wasm32-unknown-unknown/release/redact.wasm");

    private static SandboxTransform<SinkRecord> transform() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put("sandbox.provider", "wasm");
        configs.put("sandbox.module", MODULE.toString());
        configs.put("schemas.enable", false);
        final SandboxTransform<SinkRecord> transform = new SandboxTransform<>();
        transform.configure(configs);
        return transform;
    }

    private static SinkRecord record(final Map<String, Object> value) {
        return new SinkRecord("payments", 0, null, "k", null, value, 1L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void masksCardNumbersInsideTheSandbox() {
        assumeThat(Files.exists(MODULE)).as("rust guest built").isTrue();

        final Map<String, Object> value = new HashMap<>();
        value.put("card", "4111111111111111");
        value.put("amount", 1299);

        try (SandboxTransform<SinkRecord> transform = transform()) {
            final SinkRecord out = transform.apply(record(value));
            final Map<String, Object> result = (Map<String, Object>) out.value();

            assertThat(result).containsEntry("card", "************1111");
            assertThat(result).containsEntry("amount", 1299L);
            // The original record must not have been mutated: the Transformation contract
            // forbids touching anything reachable from the input.
            assertThat(value).containsEntry("card", "4111111111111111");
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void masksCardNumbersWithSchemasEnabled() {
        assumeThat(Files.exists(MODULE)).as("rust guest built").isTrue();
        // schemas.enable defaults to true, and JsonConverter then wraps the record as
        // {"schema":...,"payload":...}. A guest that only inspects the outer object would
        // silently forward the PAN untouched.
        final Map<String, Object> configs = new HashMap<>();
        configs.put("sandbox.provider", "wasm");
        configs.put("sandbox.module", MODULE.toString());
        configs.put("schemas.enable", true);

        final Schema schema = SchemaBuilder.struct()
            .field("card", Schema.STRING_SCHEMA)
            .field("amount", Schema.INT32_SCHEMA)
            .build();
        final Struct value = new Struct(schema).put("card", "4111111111111111").put("amount", 1299);
        final SinkRecord record =
            new SinkRecord("payments", 0, null, "k", schema, value, 1L);

        try (SandboxTransform<SinkRecord> transform = new SandboxTransform<>()) {
            transform.configure(configs);
            final SinkRecord out = transform.apply(record);
            assertThat(out.value().toString()).doesNotContain("4111111111111111");
            assertThat(out.value().toString()).contains("************1111");
        }
    }

    @Test
    void dropsRecordsTheGuestMarksInternal() {
        assumeThat(Files.exists(MODULE)).as("rust guest built").isTrue();

        final Map<String, Object> value = new HashMap<>();
        value.put("internal", true);

        try (SandboxTransform<SinkRecord> transform = transform()) {
            assertThat(transform.apply(record(value))).isNull();
        }
    }

    @Test
    void hasNoFilesystemOrNetworkAccess() {
        assumeThat(Files.exists(MODULE)).as("rust guest built").isTrue();
        // No WASI and no host imports are supplied, so a module that needed either would have
        // failed to instantiate. That this one loads and runs proves it reaches nothing outside
        // its own linear memory.
        try (SandboxTransform<SinkRecord> transform = transform()) {
            final Map<String, Object> value = new HashMap<>();
            value.put("card", "5500005555555559");
            assertThat(transform.apply(record(value))).isNotNull();
        }
    }
}
