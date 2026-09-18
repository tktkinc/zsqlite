# Local disk with asynchronous object storage

The intended durability contract is local first: a write succeeds after the
required local disk state is durable. Object storage may lag, but its published
root must always describe a complete, recoverable uploaded snapshot. Restoring
after loss of local disk recovers the last published remote snapshot, which may
precede the latest locally committed transaction.

## Current implementation and remaining work

The core currently supports one `StorageBackend` namespace with immutable
objects, conditional catalog publication, exact reader leases, and GC permits.
Active database state, WAL/journals, and coordination remain local. Sealed
manifests are the remote recovery boundary; uploading objects alone does not
replicate unsealed active writes or WAL transactions.

Maintenance selects sparse packs from authenticated metadata, batches compressed
frame copies, and publishes one replacement checkpoint per pass. It can collect
whole dead blobs without reading their payloads. Exact pack inspection remains a
separate operation that reads pack metadata. These improvements apply to both
warm local data and cold object storage reads.

A production tier adapter, upload snapshot protection, local eviction policy,
and remote GC are not implemented. The `gc_object_storage`
benchmark's `local_async` mode models local-first maintenance and batched remote
publication costs; it defers remote GC and is not a crash-safe tier adapter.
The [adversarial protocol tests](gc-testing.md) exercise real catalogs, leases,
collection, and snapshot restoration under controlled failure schedules. They
specify the protocol without claiming coverage of an implemented tier worker.

## Two roots and exact upload snapshots

The local catalog is authoritative for current operation. The remote catalog is
authoritative for remote recovery. Publishing either root must follow durable
installation of every dependency that root can select in its own tier.

Each upload needs the exact catalog bytes and physical dependency closure,
protected against GC for the duration of the upload. That closure includes catalog
index runs, resolved manifest ancestors, dictionaries, and the exact blob
representations selected by that catalog. Include every published head and
retained root represented by the snapshot. Capture a finalized state; a copied
pending local publication cannot require unavailable local recovery evidence.

A named `DurablePin` is insufficient: it retains a logical endpoint, whose lookup
can switch to a newer equivalent metadata checkpoint. A named head retains its
manifest but still resolves ancestors and preferred pack placements through the
current catalog. Neither mechanism preserves an older catalog's complete exact
physical dependency set. Existing physical leases can protect a running upload;
snapshot capture must cover every dependency, including all catalog index runs.
After a crash, the uploader can abandon the candidate and capture the latest
durable local state again. A durable queue and persistent snapshot pins are
optional if interrupted uploads must resume; remote consistency does not require
that promise.

## Upload and publication protocol

```text
under local catalog exclusion:
    capture finalized root S and its exact physical dependencies D
    hold exact physical leases on D; register this active upload with remote GC
release catalog exclusion
upload missing immutable objects in D, with bounded concurrent requests
confirm every required object is durable and readable remotely
compare-and-exchange remote root from its observed revision to S
resolve uncertain publication before releasing its dependency protection
ensure remote GC recognizes the confirmed root and its dependencies
release upload leases; reclaim old objects only after rechecking all roots
```

Copy existing compressed object bytes; do not decode payloads for upload. Object
keys are immutable, so retries can reuse identical completed uploads. A stale
CAS must reload the remote root and preserve newer acknowledged progress. An
uncertain CAS retains both possible dependency sets until rereading resolves it.

A crash before root publication leaves the previous remote snapshot intact.
Restart may abandon the candidate and resnapshot local state; identical completed
uploads remain reusable. Reconcile with the actual remote root before eviction
or GC. An old publication request may still be in flight after process death:
resolve or fence it before treating orphan uploads as deletable. Refreshing the
current remote root through CAS gives it a never-reused revision, fencing older
CAS requests. Recheck dependencies after that barrier. A durable outbox is an
alternative for retaining and resuming ambiguous candidates.

## Eviction and collection

Local eviction removes a local copy of a still-live immutable object. Require a
confirmed durable remote copy protected by a published snapshot or another
owner that survives restart before eviction. Local-only objects and unfinished
uploads cannot be evicted merely to meet a cache budget. Reads use local bytes
when present and otherwise fetch authenticated ranges from the remote tier.

Local GC and remote GC must use their own root sets and account for cross-tier
dependencies. Local collection protects current local roots, exact readers, and
unfinished uploads. Remote collection protects the published remote snapshot,
remote readers, in-flight uploads, and remote copies still backing live local
roots. An older remote snapshot remains protected until its successor is confirmed;
publishing a newer local checkpoint does not retire the older remote root.

Remote root replacement must also preserve metadata lookup records needed by
existing remote readers. Keeping their immutable blobs and manifests alone is
insufficient: the current collector resolves reader roots through catalog
endpoint and placement records. A locally collected catalog may have already
retired those records. Publishing it verbatim can leave old readers readable
through their existing leases while preventing any further remote collection.
Coordinate reader retention with snapshot capture, merge the required records,
or give remote readers an independently traversable exact snapshot closure
before replacing the root. The tests require this case to fail closed, with no
deletion, when lookup records have been lost.

Retiring an old remote root only makes its exclusive objects candidates for
deletion. Revalidate all relevant roots and readers before deleting. A local GC
permit must never trigger remote deletion without that remote validation. Report
local bytes freed separately from remote bytes freed; deleting one copy does not
mean that the other tier reclaimed space.

Upload, cache-fill, and deletion workers need per-object fencing or equivalent
ordering. Deletion must not race with an older upload that subsequently recreates
the object. A delayed cache fill must not restore an evicted or deleted local
entry after its ownership changed. Preserve fencing state across restart where
work can survive restart.

## Namespace and authority

Expose a stable identity for the logical tiered namespace across restart and
cache eviction. Local and remote transport identities may differ. Keep one
host's existing shared coordination directory; current leases do not provide a
distributed writer or remote-reader coordination protocol.

`DeletePermit` is namespace-bound and valid only during its catalog exclusion
lifetime. A wrapper cannot pass that permit to a child backend with a different
identity, or enqueue it for deletion after the guard ends. The future tier
implementation needs explicit physical retention and deletion authority for each
tier, with fresh validation when deferred deletion actually executes.
