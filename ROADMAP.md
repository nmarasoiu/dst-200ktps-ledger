# Roadmap beyond the core

The [development stages](README.md#14-development-stages) 0 to 7 build and prove the authorization, cancel and capture pipeline. The phases below extend it once that core holds. Each is tracked as a "Later" [milestone](https://github.com/nmarasoiu/dst-200ktps-ledger/milestones). They are listed by learning value; dependencies are noted where they exist.

Every phase follows the same rule as the core: new behavior gets invariants first, then the simulator, then real binaries.

## A. Card lifecycle

Complete what happens to a hold after authorization (README section 12, gap 3).

- Multiple captures per hold, captures above the held amount (tips), and incremental authorizations. One full or partial capture per hold is already in the core (README section 8).
- Holds longer than 7 days for merchant types that need them, such as hotels and car rental.
- Reversals and advices that arrive with network identifiers, routed by computing `A` from the identifiers they echo, with no lookup. The key derived from them must start with the original's creation time (README section 7).
- Forced holds for stand-in approvals, which may exceed the balance. Each gets its own ID; the timed-out authorization is still cancelled.
- Refunds and chargebacks as new transfers linked to the original.
- End-of-day clearing files and reconciliation against the network's settlement.

**Done when** every lifecycle path has invariants, simulator coverage and a reconciliation check that balances to zero.

## B. Sharding across TigerBeetle clusters

Go beyond one cluster's throughput and one cluster's storage. At the assumed volume the ledger needs a new cluster per period whatever its throughput (README section 4), so period switches come first in this phase and are needed for production.

- Period switches: at each switch, move each account's available balance to the new cluster while the old cluster's holds drain, and sweep the funds they release as they end (README section 12, gap 10). The simulator runs switches, with crashes and stale copies at the boundary.
- Accounts partitioned across clusters by range or hash. A cancel or a capture carries only the client key, so sharding by account would also need the account on every cancel and capture, a second field the caller must get right.
- Transfers between clusters as a protocol across shards, with pending transfers as the prepare step and a fence-style guard against stale participants.
- Rebalancing accounts between clusters without downtime.

**Depends on** stages 3 and 4: the cross-shard protocol needs the simulator first. **Done when** the simulator shows no money created or lost across shards and period switches under crashes and partitions.

## C. Formal verification

Prove the protocols, not just test them.

- A TLA+ or P model of fence-then-void and of replay-safe expiry, model-checked against the README invariants.
- A Jepsen-style checker for balance reads. Stage 6 already checks what the workload driver was told about its own requests; this checks that every balance a caller reads is explained by some order of the transfers the ledger applied. It needs the pipeline to serve reads, from TigerBeetle or from the Postgres audit log of phase E. Jepsen's Elle is the reference.
- The same model extended to the cross-shard protocol of phase B.

**Done when** the model checker finds the planted bugs of stage 3 and passes the real design.

## D. Multiple currencies

- One TigerBeetle ledger per currency.
- Conversions as linked transfers through foreign-exchange liquidity accounts.
- Rate snapshots carried in the command, so a replay uses the same rate.
- Rounding rules and minor units per currency.

**Done when** every conversion balances to zero across its linked transfers.

## E. Accounting views

Aurora PostgreSQL as the complete audit log and the long-term record (README section 4, three records), including the money movements of every retired TigerBeetle cluster, and the accounting views on top of it.

- Every command and outcome from the command log, and every ledger change from the tailer, each checked against the other. The tailer is the offline checker's scan of every transfer by timestamp, run continuously and keeping its position (README section 12, gap 4). Expiries are derived from each hold's timestamp and timeout.
- Exactly-once loading: each reader stores its position in the same Postgres transaction as the rows it writes, so a restart resumes where the last commit left off.
- Answers for keys older than every live TigerBeetle cluster: an authorization gets the decision recorded for its key, and a cancel or a capture the end of its hold (README section 4, one cluster per period). It is the only read made for a caller, off the path to a normal answer.
- Retiring a TigerBeetle cluster once the audit log has held all of it for 30 days, so every backup young enough to rebuild from holds it too (README section 4).
- Closing a period: a period closes once the audit log holds everything in it. For money, the tailer has passed the period's end in the time of every live TigerBeetle cluster; each cluster's time only grows. For decisions, on every partition the resolver's committed offset has passed the last command appended before the period's end, so each of those commands has its outcome in the log, and the loader has read up to the partition's end offset as it stood then. Reports read only closed periods, so the loader's lag delays a report but never changes what it holds.
- Append-only: audit rows are never updated or deleted. Personal data is encrypted, and erased at the end of its retention period by deleting its key (phase G).
- Rebuilding: a backup younger than the command log's 30 days, replayed from the positions stored with its rows, loses nothing (README section 4).
- Time partitions, with older ones moved to cheaper storage. EU rules keep records for 5 years after the customer relationship ends, and up to 5 more when authorities require it (README section 1). At the assumed 10k TPS daily average (README section 1) the audit log grows by about 0.9 TB a day, at about 1 KB per authorization over its life, so a 256 TiB Aurora volume holds about a year (*estimate*), and "complete" means across Aurora and the archive.
- A chart of accounts on top of TigerBeetle accounts.
- Statements, balance history, interest accrual and fees.

**Done when** the audit log's two sources agree, statements reconcile with TigerBeetle balances at any point in time, and a report for a period that closes while the loader lags waits for the close and then matches TigerBeetle.

## F. Real-time risk checks

- Velocity and spending-pattern checks at 50k TPS.
- A decision on which checks run before TigerBeetle, inside the 1.5 s budget, and which run after.
- Check results recorded in the command log for audit.

**Done when** the checks fit the latency budget measured in stage 7.

## G. Operations

- Disaster recovery across regions, with stated recovery point and recovery time objectives.
- Upgrades without downtime. IDs are time-ordered from the start (README section 7), so no live switch of the ID scheme is needed.
- Safe evolution of message formats.
- Privacy erasure against an immutable ledger, using encrypted personal data whose keys can be deleted, including the deletion EU rules require when a record's retention period ends (README section 6).
- Chaos in a Kubernetes staging cluster with Chaos Mesh or LitmusChaos, moved from stage 8: pod and node kills, partitions, I/O delay, CPU pressure and clock skew. Antithesis is optional.

**Done when** a region failover, a live upgrade and scheduled chaos runs all pass the offline checker.

## H. Deterministic simulation library

Extract the stage 3 simulator into a reusable open-source library for the JVM, since no JVM effect runtime supports deterministic simulation out of the box (README section 11).

- A seeded scheduler, simulated clock and fault injection, independent of this ledger.
- Simulated Kafka and TigerBeetle adapters as optional modules.
- The ledger as the first user and the main example.
- Compare first with OpenDST and Fray, which schedule real JVM threads deterministically (README section 15). Either could one day run the real shell under simulation, closing the gap that two interpreters leave.

**Depends on** stage 3. **Done when** a second, unrelated project uses it.

## Parking lot

Small items noticed in review. None blocks a stage; pick each up when its area is touched.

- **Start the build small ([#8](https://github.com/nmarasoiu/dst-200ktps-ledger/issues/8)).** Begin with `core` and `sim`; add `checker`, `shell`, `contract-tests` and `loadgen` when their stage starts.
- **Resolver consumer protocol ([#24](https://github.com/nmarasoiu/dst-200ktps-ledger/issues/24)).** Prefer Kafka's consumer group protocol (KIP-848), so a rebalance leaves unaffected partitions processing. With the resolver off the path to every answer, this only decides how soon stragglers get their outcomes.
- **TigerBeetle CDC.** Not needed: the tailer reads every transfer by timestamp through the Java client (README section 12, gap 4). Worth it only if expiries are wanted as records rather than derived. It publishes only over AMQP; Redpanda Connect's `tigerbeetle_cdc` input or the Go client's experimental `GetChangeEvents` would bring it to Kafka.
- **Dating keys without a time.** For a bank whose keys carry no time, for example from an earlier provider, and that cannot keep the time beside each key itself: a border module, in this repository, that records each key's first-sight time with a conditional put keyed by the key's hash, and returns it with every later command (README section 7). DynamoDB on demand would need one write per authorization and one read per capture or cancel (*estimate*), at about $0.625 per million writes (*recollection, verify*), so about $20k a month at the assumed 10k TPS daily average (*estimate*).
- **Postgres CDC, for the record.** Since Postgres 13, `max_slot_wal_keep_size` caps the WAL a lagging slot retains, trading a full disk for an invalidated slot and a re-snapshot. The case against the Postgres hub in README section 13 stands.

## Related projects

Projects that reuse the same discipline, ranked by fit:

1. **Exchange matching engine.** An order book with price-time priority: a single-threaded deterministic core, an event log, deterministic simulation and hard latency limits.
2. **Payment orchestrator.** Routing across payment providers with retries, idempotency and failover; the ledger's patterns one level up.
3. **File sync service.** Content-addressed storage, resumable uploads and conflict resolution, tested with deterministic simulation.
