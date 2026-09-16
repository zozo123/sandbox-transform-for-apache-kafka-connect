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

/**
 * Registers the two Docker-backed providers.
 *
 * <p>They share an implementation and differ only in which engine they are pointed at, which is
 * also the only thing that differs about the guarantees they can honestly claim. {@code container}
 * uses the ambient Docker socket: cgroup bounds on the host's kernel. {@code microvm} expects a
 * microVM-backed engine -- Docker Sandboxes exposes one at {@code ~/.sbx/run/d/docker.sock}, with
 * no authentication -- where the guest gets its own kernel and {@code --cpus} is a real vCPU
 * allocation.
 */
public abstract class DockerSandboxProvider implements SandboxProvider {

    @Override
    public Sandbox create(final Map<String, ?> configs) {
        final SandboxTransformConfig config = new SandboxTransformConfig(configs);
        return new DockerSandbox(
            dockerHost(config),
            config.dockerName(),
            config.dockerImage(),
            config.dockerCpus(),
            config.dockerMemory(),
            config.command(),
            config.callTimeoutMs());
    }

    abstract String dockerHost(SandboxTransformConfig config);

    /** Namespaces and cgroups on the host's kernel. */
    public static final class Container extends DockerSandboxProvider {

        @Override
        public String name() {
            return "container";
        }

        /**
         * Memory and CPU are bounded by cgroups, but syscalls are not blocked.
         *
         * <p>A container shares the host kernel and reaches it through the ordinary syscall
         * interface. Docker's default seccomp profile denies a few dozen of the several hundred
         * syscalls available, which narrows the attack surface without closing it: the guest still
         * opens files, sockets and processes inside its namespaces. That is categorically weaker
         * than the wasm provider, where no host import is supplied at all and there is no syscall
         * to make, and weaker than a virtual machine with its own kernel.
         *
         * <p>Declaring {@code blocksSyscalls=true} here would make this provider look equivalent
         * to those two in {@link SandboxCapabilities}, which is exactly the comparison an operator
         * uses to choose one. It is false, so it is not declared.
         */
        @Override
        public SandboxCapabilities capabilities() {
            return new SandboxCapabilities(
                SandboxCapabilities.Isolation.CONTAINER, true, true, false);
        }

        @Override
        String dockerHost(final SandboxTransformConfig config) {
            return config.dockerHost();
        }
    }

    /**
     * Its own kernel and its own scheduler.
     *
     * <p>This is the provider that closes the gap the WebAssembly one leaves: a guest that loops
     * forever is bounded by the virtual machine's own CPU allocation rather than holding the
     * Connect task thread.
     */
    public static final class MicroVm extends DockerSandboxProvider {

        private static final String DEFAULT_HOST =
            "unix://" + System.getProperty("user.home") + "/.sbx/run/d/docker.sock";

        @Override
        public String name() {
            return "microvm";
        }

        @Override
        public SandboxCapabilities capabilities() {
            return new SandboxCapabilities(
                SandboxCapabilities.Isolation.VIRTUAL_MACHINE, true, true, true);
        }

        @Override
        String dockerHost(final SandboxTransformConfig config) {
            final String configured = config.dockerHost();
            return configured.isEmpty() ? DEFAULT_HOST : configured;
        }
    }
}
