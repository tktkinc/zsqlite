# VFS safety boundary

`src/vfs/runtime.rs` forbids unsafe Rust. It owns database I/O decisions, the
process registry, checkpoint/publication state, cache statistics, and managed
file deletion. It accepts Rust references, slices, and scalar arguments.

`src/vfs.rs` implements SQLite's ABI: registration, callback arguments, scoped
file borrows, result conversion, and panic containment. Raw file-control
arguments, shared-memory pointers, mapping pointers, and loader handles stay in
this boundary. Unknown controls are forwarded without interpreting their data.
The boundary initializes read buffers before exposing them as Rust byte slices;
output-only scalar slots are written without reading their previous contents.

`src/vfs/parent.rs` owns the parent file allocation and any remapped SQLite
filename. Ordinary parent I/O has safe methods. Opening from foreign pointers
requires an explicit safety contract. Partial opens are cleaned up, explicit
close consumes the owner, and Drop provides cleanup during unwinding. The
filename's SQLite framing and URI parameters remain alive through parent close.

SQLite or its caller must provide exclusive access to each file during a
callback. Different files can execute concurrently: shared Store access uses
mutexes and registration data is immutable. The registration mutex does not
serialize runtime callbacks. The host must retain the selected parent VFS for
the lifetime of the shim, which is registered permanently. These requirements
also apply to custom parent VFS implementations.

Null and integer checks handle supported error cases; they cannot validate an
arbitrary foreign pointer's allocation, alignment, lifetime, or buffer size.
Those guarantees come from SQLite's callback contract or an unsafe caller.
Panic containment handles unwinding Rust panics; it does not catch undefined
behavior, process aborts, or failures inside foreign code.

Every unsafe block has an adjacent SAFETY comment stating the operation's local
justification. Unsafe functions document caller obligations. Clippy enforces
`undocumented_unsafe_blocks` across library, tests, and benchmarks. The OS syscall
wrappers and raw connection-statistics API have separate, documented boundaries.
