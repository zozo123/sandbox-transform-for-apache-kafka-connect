# sandbox-transform-for-apache-kafka-connect

> Run a Kafka Connect transform's code inside a **sandbox** — behind a pluggable provider
> interface. WebAssembly and Docker Sandboxes ship as the two worked examples.

A custom single-message transform today means writing Java, building a JAR, getting it onto every
worker's `plugin.path`, and restarting the workers. This makes the transform's logic a *sandboxed
guest* instead: supplied as configuration, written in any language, running behind a boundary.

```properties
transforms=redact
transforms.redact.type=dev.islo.kafka.connect.sandbox.SandboxTransform
transforms.redact.sandbox.provider=wasm
transforms.redact.sandbox.module=/opt/transforms/redact.wasm
```

## The abstraction is the point

```
Connect ──apply(record)──▶ SandboxTransform ──▶ Sandbox ──▶ guest code
                                                   ▲
                     wasm │ microvm │ container │ subprocess │ yours, via ServiceLoader
```

Two interfaces, both small:

```java
public interface Sandbox extends Closeable {      // data plane, one record at a time
    byte[] call(byte[] request);
    boolean isAlive();
}

public interface SandboxProvider {                // control plane, once per task
    String name();
    SandboxCapabilities capabilities();
    Sandbox create(Map<String, ?> configs);
}
```

A third party adds a boundary — gVisor, Firecracker, E2B — by dropping a JAR next to this one.
Nothing here changes.

## The providers

A sandbox is not one thing. These are the same idea at different points on the isolation curve, and
the transform above them is identical.

| | `wasm` | `microvm` | `container` | `subprocess` |
|---|---|---|---|---|
| boundary | WebAssembly in the worker JVM | own kernel, own scheduler | namespaces + cgroups | a child process |
| bounds memory | yes | yes | yes | **no** |
| blocks syscalls | yes | yes | **partly** | **no** |
| **bounds CPU** | **no** | **yes** | yes | **no** |
| cost / record | **5.75 µs** | ~120–370 µs | ~370 µs | 68 µs |
| deploys as | a plain JAR, anywhere | needs a Docker engine | needs a Docker socket | anywhere |

`wasm` is the default because it is the only one that stays an ordinary JAR: production Connect runs
containerised under Strimzi, Aiven and Confluent, where nothing reaches a Docker socket.

`microvm` and `container` share one implementation and differ only in which engine they are pointed
at — which is also the only thing that differs about what they can honestly claim.
[Docker Sandboxes](https://www.docker.com/products/docker-sandboxes/) exposes a microVM-backed
Docker engine at `~/.sbx/run/d/docker.sock`; against it the guest gets its own kernel and `--cpus`
becomes a real vCPU allocation rather than a share of host time. Verified on this machine: kernel
`7.0.12` inside against `6.12.54-linuxkit` for an ordinary container, and `nproc` of 1 under
`--cpus 1` on a 10-core host.

That is what buys the one guarantee WebAssembly cannot give — a guest that loops forever is capped
rather than holding the Connect task thread — and it costs roughly two orders of magnitude per
record.

`subprocess` is **not** a sandbox and says so in its capabilities. It is the latency floor a real
boundary must beat.

> **Why the Engine API and not the `sbx` CLI.** Every `sbx` lifecycle command requires an
> interactive Docker sign-in (`401 ... user is not authenticated to Docker`), which a headless
> Connect worker cannot perform — a provider built on it would be dead code everywhere but a
> developer's signed-in laptop. The engine socket answers unauthenticated. Docker Sandboxes itself
> is proprietary and is not bundled; whatever `docker` is on `PATH` is used.

## Measured, not asserted

`Transformation.apply` is per-record and synchronous — no batching at this layer — so the boundary
is paid once per record. 200k records after 20k warmup; `./gradlew benchmark`.

| boundary | p50 | max rec/s |
|---|---:|---:|
| wasm, empty guest | 0.08 µs | 12,050,902 |
| wasm + Connect JSON codec | 1.17 µs | 264,886 |
| **wasm, real Rust guest, end to end** | **5.75 µs** | **159,571** |
| subprocess (no isolation) | 68.13 µs | 14,108 |
| microvm / container (persistent `exec` stream) | ~120–370 µs | ~2,700–8,300 |

Two findings shaped the design. **The isolation is cheaper than the serialization** — the wasm
boundary costs 0.08 µs while the JSON codec around it costs an order of magnitude more. And **AOT
compilation is not optional**: interpreted, the real guest cost 300 µs/record; compiled to JVM
bytecode, 5.75 µs. Reproduced on Linux CI.

## Guest ABI

```
alloc(size: i32) -> ptr: i32
transform(ptr: i32, len: i32) -> packed: i64   // (out_ptr << 32) | out_len
reset()                                        // optional, strongly recommended
```

Request and response are JSON, using Connect's own `JsonConverter` mapping so `Struct`s and logical
types round-trip losslessly.

```jsonc
// host → guest
{"topic":"payments","partition":0,"value":{"card":"4111111111111111"}}

// guest → host, exactly one of:
{"value":{"card":"************1111"}}   // transform; add "topic" to reroute
{"drop":true}                           // filter out
{"error":"unsupported schema"}          // reject this record
```

**Mind the schema envelope.** With `schemas.enable=true` (the default) `JsonConverter` wraps the
record as `{"schema":…,"payload":…}`. A guest that inspects only the outer object silently forwards
the record untouched — for a redaction transform, leaking exactly what it exists to mask. The Rust
example in [`examples/redact-rs`](examples/redact-rs) resolves the data node first; there is a
regression test for it.

## Errors

| situation | exception | effect with `errors.tolerance=none` (default) | with `errors.tolerance=all` |
|---|---|---|---|
| guest returns `{"error":…}` | `DataException` | task fails | record skipped |
| guest traps, or the boundary breaks | `SandboxException` | task fails | record skipped — see below |

Connect decides what is tolerable by *stage*, not by exception type, so `errors.tolerance=all`
swallows a `SandboxException` from a broken boundary exactly as it swallows a `DataException` from
a rejected record. There is no setting that skips bad records but stops the task when the sandbox
itself breaks; if that distinction matters, run with `errors.tolerance=none` and let the task fail.

**The dead letter queue is sink-only.** Connect installs a `DeadLetterQueueReporter` for sink tasks
and only a `LogReporter` for source tasks, and the `errors.deadletterqueue.*` properties are defined
on `SinkConnectorConfig` alone. On a source connector a rejected record is logged and dropped.
Note also that the DLQ stores the message *as it arrived*, before this transform ran — so for a
redaction transform the DLQ holds the unmasked original.

Tombstones pass through untouched; dropping them would break compaction downstream.

## Build and verify

```bash
./gradlew build              # 25 tests, checkstyle
./gradlew integrationTest    # real Kafka broker + real Connect worker via Testcontainers
./gradlew benchmark
```

`SandboxTransformIT` loads the transform from `plugin.path` through Connect's own plugin
classloader and runs the real Rust guest against a real broker.

## Honest limitations

- **`wasm` cannot bound CPU.** A guest looping forever holds the task thread, and a Java thread
  cannot be safely killed. Chicory's only hook is interpreter-only and experimental. Use `sbx` for
  code you did not compile.
- **The Docker providers need an engine.** They shell out to `docker`, so the worker must be able
  to reach a Docker socket — which containerised Connect deployments will not grant. They are
  development and evaluation providers, and they cannot run in this project's CI (GitHub runners
  offer no nested virtualisation).
- **`DockerSandboxIT` is skipped** unless a Docker Sandboxes engine socket is reachable. The
  command sequence it issues is verified by hand on the host; the test itself cannot run inside a
  containerised build on macOS, where unix sockets do not cross the VM boundary.
- Value transforms only — no key or header rewriting, no `Predicate` support yet.

## On upstreaming

A Connect transform is already a plugin, so this installs into any worker with nobody's approval.
That is the point, and it is why there is no PR to open against
[apache/kafka](https://github.com/apache/kafka) today: new public interfaces there require a
[KIP](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals) — a `[DISCUSS]`
thread and a vote on `dev@kafka.apache.org` — before any code is reviewed. The Docker providers could never ship in
Apache Kafka regardless, depending as they do on an external engine.

If the idea earns a KIP, this repository is the reference implementation it would point at.

## License

Apache-2.0.

---

Developed with the assistance of Claude (Anthropic); all code was reviewed by a human maintainer
who takes responsibility for it.
