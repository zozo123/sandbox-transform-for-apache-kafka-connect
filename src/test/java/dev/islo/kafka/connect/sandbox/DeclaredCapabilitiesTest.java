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

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the capability declarations that an operator uses to choose a boundary.
 *
 * <p>{@link SandboxCapabilities} is the whole point of the provider SPI: it is how a deployment
 * compares boundaries without reading their source. A provider that overstates itself there is
 * worse than one that does not exist, because the overstatement is what gets trusted. These tests
 * exist so that a claim cannot be widened without someone deciding to widen it.
 */
class DeclaredCapabilitiesTest {

    private static SandboxCapabilities capabilitiesOf(final String name) {
        final Map<String, SandboxProvider> byName = new HashMap<>();
        for (final SandboxProvider provider
                 : ServiceLoader.load(SandboxProvider.class,
                     SandboxProvider.class.getClassLoader())) {
            byName.put(provider.name(), provider);
        }
        assertThat(byName).containsKey(name);
        return byName.get(name).capabilities();
    }

    @Test
    void theContainerProviderDoesNotClaimToBlockSyscalls() {
        // A container reaches the host kernel through the ordinary syscall interface. Docker's
        // default seccomp profile denies a few dozen of several hundred syscalls, which narrows
        // the surface without closing it. Claiming otherwise would put this provider level with
        // wasm (no host imports at all) and with a virtual machine (its own kernel) in the one
        // comparison an operator actually reads.
        final SandboxCapabilities container = capabilitiesOf("container");

        assertThat(container.isolation()).isEqualTo(SandboxCapabilities.Isolation.CONTAINER);
        assertThat(container.blocksSyscalls()).isFalse();

        // What it does bound, it really does bound: cgroups are real.
        assertThat(container.boundsMemory()).isTrue();
        assertThat(container.boundsCpu()).isTrue();
    }

    @Test
    void onlyTheOutOfProcessRuntimesBoundCpu() {
        // The honest limitation of the default runtime, and the reason the SPI is pluggable at
        // all. If this ever flips to true, something has to have changed about how a wasm guest
        // is scheduled -- it should not flip quietly.
        assertThat(capabilitiesOf("wasm").boundsCpu()).isFalse();

        assertThat(capabilitiesOf("microvm").boundsCpu()).isTrue();
        assertThat(capabilitiesOf("container").boundsCpu()).isTrue();
    }
}
