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

package dev.islo.kafka.connect.sandbox.provider;

import java.util.Map;

import dev.islo.kafka.connect.sandbox.Sandbox;
import dev.islo.kafka.connect.sandbox.SandboxCapabilities;
import dev.islo.kafka.connect.sandbox.SandboxProvider;
import dev.islo.kafka.connect.sandbox.SandboxTransformConfig;

/** Registers the {@code subprocess} runtime. */
public class SubprocessSandboxProvider implements SandboxProvider {

    @Override
    public String name() {
        return "subprocess";
    }

    @Override
    public SandboxCapabilities capabilities() {
        // No boundary at all: the child inherits the worker's user, filesystem and network.
        // Present so the transform can say so out loud rather than implying isolation it lacks.
        return new SandboxCapabilities(
            SandboxCapabilities.Isolation.HOST_PROCESS, false, false, false);
    }

    @Override
    public Sandbox create(final Map<String, ?> configs) {
        final SandboxTransformConfig config = new SandboxTransformConfig(configs);
        return new SubprocessSandbox(config.command(), config.callTimeoutMs());
    }
}
