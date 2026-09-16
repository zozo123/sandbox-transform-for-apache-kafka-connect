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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Measures what one sandbox boundary crossing costs per record.
 *
 * <p>{@code Transformation.apply} is per-record and synchronous, so this cost is paid once for
 * every record a task processes and it is the number that decides whether a runtime is usable.
 * The guests do no work at all, which is deliberate: what is being measured is the boundary, not
 * the transform.
 *
 * <p>Run with {@code ./gradlew benchmark}.
 */
public final class BoundaryCostBenchmark {

    private static final int WARMUP = 20_000;
    private static final int ITERATIONS = 200_000;
    private static final String RESPONSE = "{\"value\":{\"redacted\":true}}";

    private BoundaryCostBenchmark() {
    }

    public static void main(final String[] args) throws Exception {
        final Path dir = Files.createTempDirectory("sandbox-bench");
        final Path module = WasmGuests.returning(dir, "bench", RESPONSE);

        System.out.println("Records per measurement: " + ITERATIONS + " (after " + WARMUP
            + " warmup), guest work: none");
        System.out.printf(Locale.ROOT, "%-34s %9s %9s %9s %9s %14s%n",
            "boundary", "p50", "p90", "p99", "mean", "max rec/s");
        System.out.println("-".repeat(90));

        benchRuntime("wasm (chicory, in-process)",
            new dev.islo.kafka.connect.sandbox.provider.WasmSandbox(module.toString(), 64));

        final Path guest = shellGuest(dir);
        benchRuntime("subprocess (sh, no isolation)",
            new dev.islo.kafka.connect.sandbox.provider.SubprocessSandbox(
                Arrays.asList("/bin/sh", guest.toString()), 5000));

        benchTransform("wasm + JSON codec (full apply)", module);

        final Path rustGuest =
            Paths.get("examples/redact-rs/target/wasm32-unknown-unknown/release/redact.wasm");
        if (Files.exists(rustGuest)) {
            benchTransform("  ... with the real Rust guest", rustGuest);
        } else {
            System.out.println("  (build examples/redact-rs to include the real guest)");
        }
        System.out.println();
        System.out.println("Note: subprocess provides no isolation. It is included as the "
            + "latency floor for an out-of-process boundary.");
    }

    private static void benchRuntime(final String label, final Sandbox runtime) {
        final byte[] request =
            "{\"topic\":\"orders\",\"value\":{\"card\":\"4111111111111111\"}}"
                .getBytes(StandardCharsets.UTF_8);
        try {
            for (int i = 0; i < WARMUP; i++) {
                runtime.call(request);
            }
            final long[] samples = new long[ITERATIONS];
            for (int i = 0; i < ITERATIONS; i++) {
                final long start = System.nanoTime();
                runtime.call(request);
                samples[i] = System.nanoTime() - start;
            }
            report(label, samples);
        } finally {
            runtime.close();
        }
    }

    private static void benchTransform(final String label, final Path module) {
        final Map<String, Object> configs = new HashMap<>();
        configs.put("sandbox.provider", "wasm");
        configs.put("sandbox.module", module.toString());
        configs.put("schemas.enable", false);

        final Map<String, Object> value = new HashMap<>();
        value.put("card", "4111111111111111");
        value.put("amount", 1299);
        final SinkRecord record = new SinkRecord("orders", 0, null, "k", null, value, 42L);

        final SandboxTransform<SinkRecord> transform = new SandboxTransform<>();
        try {
            transform.configure(configs);
            for (int i = 0; i < WARMUP; i++) {
                transform.apply(record);
            }
            final long[] samples = new long[ITERATIONS];
            for (int i = 0; i < ITERATIONS; i++) {
                final long start = System.nanoTime();
                transform.apply(record);
                samples[i] = System.nanoTime() - start;
            }
            report(label, samples);
        } finally {
            transform.close();
        }
    }

    private static void report(final String label, final long[] samplesNanos) {
        Arrays.sort(samplesNanos);
        final double p50 = samplesNanos[(int) (samplesNanos.length * 0.50)] / 1000.0;
        final double p90 = samplesNanos[(int) (samplesNanos.length * 0.90)] / 1000.0;
        final double p99 = samplesNanos[(int) (samplesNanos.length * 0.99)] / 1000.0;
        long total = 0;
        for (final long sample : samplesNanos) {
            total += sample;
        }
        final double mean = total / (double) samplesNanos.length / 1000.0;
        System.out.printf(Locale.ROOT, "%-34s %8.2fus %8.2fus %8.2fus %8.2fus %14s%n",
            label, p50, p90, p99, mean, String.format(Locale.ROOT, "%,d", (long) (1_000_000 / mean)));
    }

    private static Path shellGuest(final Path dir) throws IOException {
        final Path script = dir.resolve("guest.sh");
        Files.write(script, ("while IFS= read -r line; do printf '%s\\n' '" + RESPONSE
            + "'; done\n").getBytes(StandardCharsets.UTF_8));
        return script;
    }
}
