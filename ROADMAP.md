# Roadmap beyond the core

The [development stages](README.md#14-development-stages) 0 to 8 build and prove the authorization, cancel and capture pipeline. The phases below extend it once that core holds. Each is tracked as a "Later" [milestone](https://github.com/nmarasoiu/dst-200ktps-ledger/milestones). They are listed by learning value; dependencies are noted where they exist.

Every phase follows the same rule as the core: new behavior gets invariants first, then the simulator, then real binaries.

## A. Card lifecycle

Complete what happens to a hold after authorization (README section 12, gap 3).

- Multiple captures per hold, captures above the held amount (tips), and incremental authorizations. One full or partial capture per hold is already in the core (README section 8).
- Holds longer than 7 days for merchant types that need them, such as hotels and car rental.
- Reversals and advices that arrive with network identifiers, routed by computing `A` from the identifiers they echo, with no lookup.
- Forced holds for stand-in approvals, which may exceed the balance. Each gets its own ID; the timed-out authorization is still cancelled.
- Refunds and chargebacks as new transfers linked to the original.
- End-of-day clearing files and reconciliation against the network's settlement.

**Done when** every lifecycle path has invariants, simulator coverage and a reconciliation check that balances to zero.

## B. Sharding across TigerBeetle clusters

Go beyond one cluster's throughput.

- Accounts partitioned across clusters by range or hash.
- Transfers between clusters as a protocol across shards, with pending transfers as the prepare step and a fence-style guard against stale participants.
- Rebalancing accounts between clusters without downtime.

**Depends on** stages 3 and 4: the cross-shard protocol needs the simulator first. **Done when** the simulator shows no money created or lost across shards under crashes and partitions.

## C. Formal verification

Prove the protocols, not just test them.

- A TLA+ or P model of fence-then-void and of replay-safe expiry, model-checked against the README invariants.
- A Jepsen-style history checker that records what every client saw and checks it against the ledger.
- The same model extended to the cross-shard protocol of phase B.

**Done when** the model checker finds the planted bugs of stage 3 and passes the real design.

## D. Multiple currencies

- One TigerBeetle ledger per currency.
- Conversions as linked transfers through foreign-exchange liquidity accounts.
- Rate snapshots carried in the command, so a replay uses the same rate.
- Rounding rules and minor units per currency.

**Done when** every conversion balances to zero across its linked transfers.

## E. Accounting views

- A chart of accounts on top of TigerBeetle accounts.
- Statements, balance history, interest accrual and fees.
- The Postgres projection fed by the results topic, with expiries derived from each hold's timestamp and timeout (README section 12, gap 4), and balances reconciled against TigerBeetle.

**Done when** statements reconcile with TigerBeetle balances at any point in time.

## F. Real-time risk checks

- Velocity and spending-pattern checks at 50k TPS.
- A decision on which checks run before TigerBeetle, inside the 1.5 s budget, and which run after.
- Check results recorded in the results topic for audit.

**Done when** the checks fit the latency budget measured in stage 7.

## G. Operations

- Disaster recovery across regions, with stated recovery point and recovery time objectives.
- Upgrades without downtime, including a live switch from hashed to time-ordered IDs.
- Safe evolution of message formats.
- Privacy erasure against an immutable ledger, using encrypted personal data whose keys can be deleted.

**Done when** a region failover and a live upgrade both pass the offline checker.

## H. Deterministic simulation library

Extract the stage 3 simulator into a reusable open-source library for the JVM, since no JVM effect runtime supports deterministic simulation out of the box (README section 11).

- A seeded scheduler, simulated clock and fault injection, independent of this ledger.
- Simulated Kafka and TigerBeetle adapters as optional modules.
- The ledger as the first user and the main example.

**Depends on** stage 3. **Done when** a second, unrelated project uses it.

## Parking lot

Small items noticed in review. None blocks a stage; pick each up when its area is touched.

- **Start the build small (#8).** Begin with `core` and `sim`; add `checker`, `shell`, `contract-tests` and `loadgen` when their stage starts.
- **One results topic first.** The slim replies topic saves broadcast egress only at fleet sizes stage 7 will measure; until then the edge can filter the results topic by a request ID header.
- **Batcher consumer protocol (#24).** Prefer Kafka's consumer group protocol (KIP-848), so a rebalance leaves unaffected partitions processing. fs2-kafka 4.x folds transactional producing into `KafkaProducer`; #24's wording predates it.
- **TigerBeetle CDC as an audit feed.** Not needed for the projection (README section 12, gap 4). Worth it only if something besides the pipeline writes to TigerBeetle, or as an independent record for the history checker (phase C). Two ways into Kafka, skipping RabbitMQ: Redpanda Connect's `tigerbeetle_cdc` input with a Kafka output (beta; cgo builds only; progress kept in an external cache; licence to check), or a small bridge of our own on the Go client's experimental `GetChangeEvents`, keeping its progress in Kafka itself. The Java client has only the operation code, not a method.
- **Postgres CDC, for the record.** Since Postgres 13, `max_slot_wal_keep_size` caps the WAL a lagging slot retains, trading a full disk for an invalidated slot and a re-snapshot. The case against the Postgres hub in README section 13 stands.

## Related projects

Projects that reuse the same discipline, ranked by fit:

1. **Exchange matching engine.** An order book with price-time priority: a single-threaded deterministic core, an event log, deterministic simulation and hard latency limits.
2. **Payment orchestrator.** Routing across payment providers with retries, idempotency and failover; the ledger's patterns one level up.
3. **File sync service.** Content-addressed storage, resumable uploads and conflict resolution, tested with deterministic simulation.
