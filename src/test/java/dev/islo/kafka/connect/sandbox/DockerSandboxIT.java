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
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Proves the microvm provider actually runs, rather than merely compiling.
 *
 * <p>Skipped unless Docker Sandboxes' engine socket is present. It needs no Docker sign-in: the
 * engine at {@code ~/.sbx/run/d/docker.sock} answers unauthenticated, which is precisely why the
 * provider is built on the Engine API rather than on the {@code sbx} CLI, whose every lifecycle
 * command requires an interactive login a Connect worker cannot perform.
 */
class DockerSandboxIT {

    private static final String SOCKET = System.getProperty("sandbox.docker.socket",
        System.getProperty("user.home") + "/.sbx/run/d/docker.sock");

    /** Reads one JSON envelope per line and masks the card, honouring the schema envelope. */
    private static final String GUEST =
        "import sys,json\n"
            + "for line in sys.stdin:\n"
            + "    env=json.loads(line)\n"
            + "    v=env.get('value')\n"
            + "    d=v['payload'] if isinstance(v,dict) and 'schema' in v and 'payload' in v else v\n"
            + "    if isinstance(d,dict) and 'card' in d: d['card']='****'\n"
            + "    sys.stdout.write(json.dumps({'value':v})+'\\n'); sys.stdout.flush()\n";

    @Test
    @SuppressWarnings("unchecked")
    void masksCardNumbersInsideAMicroVm() {
        assumeThat(Files.exists(Paths.get(SOCKET))).as("Docker Sandboxes engine").isTrue();

        final Map<String, Object> configs = new HashMap<>();
        configs.put("sandbox.provider", "microvm");
        configs.put("sandbox.docker.host", "unix://" + SOCKET);
        configs.put("sandbox.docker.image", "python:3.12-slim");
        configs.put("sandbox.docker.cpus", "1");
        configs.put("sandbox.docker.memory", "512m");
        configs.put("sandbox.command", String.join(",",
            Arrays.asList("python3", "-u", "-c", GUEST)));
        configs.put("schemas.enable", false);
        configs.put("sandbox.call.timeout.ms", 30_000L);

        final Map<String, Object> value = new HashMap<>();
        value.put("card", "4111111111111111");

        try (SandboxTransform<SinkRecord> transform = new SandboxTransform<>()) {
            transform.configure(configs);
            final SinkRecord out =
                transform.apply(new SinkRecord("payments", 0, null, "k", null, value, 1L));
            assertThat((Map<String, Object>) out.value()).containsEntry("card", "****");
        }
    }
}
