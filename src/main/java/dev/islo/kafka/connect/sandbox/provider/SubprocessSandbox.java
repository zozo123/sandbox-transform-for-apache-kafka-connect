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

import java.util.List;

import dev.islo.kafka.connect.sandbox.Sandbox;
import dev.islo.kafka.connect.sandbox.SandboxException;

/**
 * Runs the guest as an ordinary child process of the Connect worker.
 *
 * <p><b>This provider is not a sandbox.</b> The child inherits the worker's user, filesystem and
 * network. It is kept for two honest reasons: a guest can be written in any language with no
 * toolchain, and it is the latency floor a real boundary must be measured against. Anything
 * claiming isolation should cost more than this. Do not use it for untrusted code.
 */
public class SubprocessSandbox implements Sandbox {

    private final StdioChannel channel;

    public SubprocessSandbox(final List<String> command, final long timeoutMs) {
        if (command == null || command.isEmpty()) {
            throw new SandboxException("The subprocess provider requires sandbox.command");
        }
        this.channel = new StdioChannel(command, timeoutMs);
    }

    @Override
    public byte[] call(final byte[] request) {
        return channel.call(request);
    }

    @Override
    public boolean isAlive() {
        return channel.isAlive();
    }

    @Override
    public void close() {
        channel.close();
    }
}
