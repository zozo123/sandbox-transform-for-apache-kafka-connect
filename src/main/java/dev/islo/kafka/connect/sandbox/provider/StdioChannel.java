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

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import dev.islo.kafka.connect.sandbox.SandboxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One request per line in, one response per line out, over a child process's stdio.
 *
 * <p>Shared by every out-of-process provider. The subtleties here are not specific to any one of
 * them: a guest that prints a start-up banner, answers twice, or closes stderr breaks the
 * conversation in exactly the same way whether it is an ordinary subprocess or a process inside a
 * sandbox, so the handling belongs in one place with one set of tests.
 */
final class StdioChannel implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StdioChannel.class);

    private final Process process;
    private final OutputStream toGuest;
    private final BlockingQueue<String> fromGuest = new ArrayBlockingQueue<>(1);
    private final AtomicBoolean poisoned = new AtomicBoolean(false);
    // Strictly one line in, one line out. Tracking whether a request is outstanding is what stops
    // an unsolicited line from being read as the answer to a LATER record.
    private final AtomicBoolean awaitingResponse = new AtomicBoolean(false);
    private final long timeoutMs;

    StdioChannel(final List<String> command, final long timeoutMs) {
        this(command, timeoutMs, new String[0]);
    }

    StdioChannel(final List<String> command, final long timeoutMs, final String[] env) {
        if (command == null || command.isEmpty()) {
            throw new SandboxException("A command is required to start the guest");
        }
        this.timeoutMs = timeoutMs;
        try {
            final ProcessBuilder builder = new ProcessBuilder(command);
            for (int i = 0; i < env.length; i += 2) {
                builder.environment().put(env[i], env[i + 1]);
            }
            this.process = builder.start();
        } catch (final IOException e) {
            throw new SandboxException("Could not start guest " + command, e);
        }
        this.toGuest = new BufferedOutputStream(process.getOutputStream());
        drain("sandbox-stdout", process.getInputStream(), true);
        drain("sandbox-stderr", process.getErrorStream(), false);
    }

    byte[] call(final byte[] request) {
        if (poisoned.get()) {
            throw new SandboxException("Sandbox is no longer usable");
        }
        awaitingResponse.set(true);
        try {
            toGuest.write(request);
            toGuest.write('\n');
            toGuest.flush();
        } catch (final IOException e) {
            awaitingResponse.set(false);
            poisoned.set(true);
            throw new SandboxException("Could not write to the guest", e);
        }

        final String line;
        try {
            line = fromGuest.poll(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            poisoned.set(true);
            throw new SandboxException("Interrupted waiting for the guest", e);
        }
        // Deliberately no `awaitingResponse.set(false)` here. The reader thread clears the flag
        // when it claims the request, so clearing it again from this side would reopen the race
        // it exists to close. Every path that leaves the flag set also poisons the channel, so it
        // is never observed stale by a later call.

        if (line == null) {
            // A late reply would be mistaken for the answer to the *next* record, so the channel
            // can never be trusted again.
            poisoned.set(true);
            throw new SandboxException(
                "Guest did not answer within " + timeoutMs + "ms; channel abandoned");
        }
        return line.getBytes(StandardCharsets.UTF_8);
    }

    boolean isAlive() {
        return !poisoned.get() && process.isAlive();
    }

    @Override
    public void close() {
        poisoned.set(true);
        try {
            toGuest.close();
        } catch (final IOException e) {
            log.debug("Ignoring error closing guest stdin", e);
        }
        try {
            if (!process.waitFor(2, TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
    }

    private void drain(final String name, final InputStream stream, final boolean isStdout) {
        final Thread thread = new Thread(() -> {
            try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!isStdout) {
                        log.warn("[sandbox] {}", line);
                        continue;
                    }
                    // Claim the outstanding request and hand the line over as one atomic step.
                    // Testing the flag and then queueing would be check-then-act: a guest that
                    // answers twice can have its second line pass the test before call() clears
                    // the flag, and that line is then delivered as the answer to the NEXT record.
                    // The result is valid JSON, correctly masked, and about a different record --
                    // silent data corruption rather than a visible failure. The queue holds one
                    // element, so a failed offer means a line is already waiting, which is the
                    // same protocol violation seen from the other side.
                    if (!awaitingResponse.compareAndSet(true, false) || !fromGuest.offer(line)) {
                        log.error("Guest wrote to stdout with no request outstanding; abandoning "
                            + "the channel. Offending line: {}", line);
                        poisoned.set(true);
                        return;
                    }
                }
            } catch (final IOException e) {
                log.debug("Guest stream {} closed", name, e);
            } finally {
                // Only stdout closing ends the conversation. A guest may close stderr and keep
                // serving perfectly well.
                if (isStdout) {
                    poisoned.set(true);
                }
            }
        }, name);
        thread.setDaemon(true);
        thread.start();
    }
}
