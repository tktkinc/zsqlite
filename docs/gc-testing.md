# Adversarial GC and tier publication tests

The GC tests run in ordinary `cargo test`, in both default and static builds.
CI runs both configurations on Linux and macOS. Nothing in this matrix requires
a benchmark invocation, cloud credentials, or an ignored test.

## Implemented storage paths

| Suite | Failures and schedules | Required result |
| --- | --- | --- |
| `src/storage/gc_batch_tests.rs` | Every reached backend operation in a multi-output repack and standalone collection; lost catalog publication responses; bounded work; reader release | Acknowledged bytes, history, and transaction ID survive errors and reopen; live readers remain readable; retries can reclaim obsolete objects |
| `src/storage/gc_crash_tests.rs` | Every occurrence of instrumented object installation, catalog publication, active replacement, and deletion boundaries, for intact and partial frames | The child exits without destructors; a fresh opener recovers the acknowledged endpoint, retries maintenance, and reclaims installed orphan objects |
| `src/storage/gc_stream_tests.rs` | Partial object writes, failure before finish, lost response after durable installation, repeated short writes | Incomplete objects never become dependencies of the published head; completed unacknowledged objects are safe to retry or reclaim |
| `src/storage/gc_remote_tests.rs` | Damage to every surviving header and payload in a coalesced read, malformed range replies, request-count checks | Corruption cannot advance publication or authorize deletion; collection/no-op passes avoid blob reads; repacking coalesces useful reads |

Operation sweeps discover counts from a successful pass, then inject at each
position. They cover later replacement packs, catalog index writes, and GC
retirement rather than assuming that the first write or publication is enough.
Stream faults follow writer start order, including overlapping writers that
finish in a different order. Each injected case must actually reach its fault.

The process-exit matrix currently exercises 108 exits across the two frame
layouts. Each child has a watchdog. Cases restore the same acknowledged fixture
at the same paths with all database handles closed, then use fresh child/open
state. These tests exercise process death; they do not simulate power loss,
filesystem write reordering, or a failed disk. Orphan reclamation assertions
cover installed objects in the backend inventory, not abandoned temporary
staging files outside that inventory.

The seeded history test independently models bytes in a vector. Three fixed
seeds run 48 steps each with 4, 8, and 16 KiB frames and page caching disabled.
Histories mix page and partial-page writes, truncation and zero-filled regrowth,
active and sealed commits, retained roots, exact readers, writable forks,
bounded maintenance, collection, and reopen. Every step compares current and
retained data against the independent model. Existing metadata corruption,
stale-writer, cross-process lease, and SQLite recovery tests also continue to run.

## Proposed two-tier protocol

`src/storage/tier_protocol_tests.rs` is an executable protocol specification,
not a production tier adapter. It uses real filesystem-backed local databases,
catalogs, immutable objects, exact OS leases, backend CAS, collection, and
bootstrap. A remote snapshot is checked by copying only the remote namespace
into a separate backend, restoring it, verifying it, and comparing its bytes
with the expected data. This keeps the verification oracle independent of local
caches and avoids changing the remote revision under test.

The schedules cover missing dependencies, interrupted upload prefixes in both
orders, duplicate uploads, failed requests, stale and duplicate publications,
uncertain publication followed by failed reconciliation, delayed CAS requests
after restart, exact upload leases surviving local repack, lagging remote roots,
remote readers, named heads, retained ancestors, and dictionary dependencies.
An abandoned-worker schedule releases every local upload lease, allows local
GC to reclaim the interrupted candidate, then publishes a fresh snapshot of
the latest local state while fencing the old request. Partial-upload schedules
also assert that uploaded objects survive collection while their owner is live.
Negative controls deliberately publish incomplete snapshots or misuse a fresh
CAS revision and require actual restore/verification to reject the result.

One regression checks that retaining remote reader blobs alone is insufficient
when root replacement drops their catalog lookup records. Collection must fail
before deletion in that case. Coordinated retention preserves the lookup records
and permits collection while readers remain active. The requirement is recorded
in [the tiering contract](tiered-storage.md).

These tests do not establish the safety of an unimplemented asynchronous worker,
eviction policy, cache-fill fencing, distributed readers, or a cloud transport.
Those implementations must run the same invariants against their actual request
and restart paths. The [performance benchmark](gc-performance.md) remains
separate from the correctness suite; elapsed-time measurements are not CI gates.

## Running the focused suites

```sh
cargo test --lib storage::gc_ -- --nocapture
cargo test --lib storage::tier_protocol_tests -- --nocapture
cargo test --no-default-features --features static
cargo clippy --all-targets --no-default-features --features static -- -D warnings
```

Fault failures identify the operation and occurrence; crash failures also report
the frame layout and child output. Seeded histories use fixed seeds so their
operation sequence can be reproduced without relying on thread timing.
