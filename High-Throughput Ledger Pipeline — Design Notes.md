# High-Throughput Ledger Pipeline — Design Notes

Oct 26, 2026 · @Dumitru-Nicolae Marasoiu

## Context and goals

A hobby design for a ledger pipeline that sustains 200,000 transactions per second (TPS), with 300,000 TPS as the stress scenario, and stays correct under crash-stop failures.

- **Domain:** card authorization style debits. The issuer must answer within a hard deadline, and holds (pending amounts) are later posted or voided.
- **Setting:** a hypothetical core-banking platform serving a large bank. Clients may be async-capable or may insist on a synchronous call.
- **Rate definition:** 200k TPS as an average over a 1-minute window. Sub-second bursts of 2 to 3 times that are absorbed by queues (the burst factor is an assumption, not a measured figure).
- **Correctness bar:** no double debit, no lost result, and a deterministic outcome for every request, even when any process crashes at any point.
- **Building blocks:** Kafka (logs), TigerBeetle (ledger and money invariants), Postgres (queryable projection), Kafka Streams with RocksDB (local key-value state).

## Design philosophy

Each stage does one clear thing against one clear store, commits its progress, and hands over to the next stage through a log.

1. **One stage, one job, one store.** Each store serves a single kind of operation, so a failure or overload in one kind does not swamp the others.
2. **Log intent before effect.** Every request is durable in Kafka before it touches the ledger, so every effect can be found and reconciled later.
3. **Batch everywhere, idempotent per item.** Throughput comes from batches; safety comes from each item carrying its own idempotency ID.
4. **Nothing on the path to an effect depends on a non-replayable decision.** Any ID that reaches TigerBeetle must already be committed in a log, or be re-derivable from it.
5. **Recovery is the normal path.** Replay after a crash runs the same code as normal processing, so the recovery logic is exercised constantly instead of rarely.
6. **Keep headroom.** A store running at its limit has no room for bursts, failover or a lagging consumer. TigerBeetle's own guidance is to run well below capacity.
7. **Count the couplings.** Every system on the hot path multiplies availability: three systems at 99.99% each give about 99.97% combined. Removing a system beats tuning one.

## From 300k TPS to the architecture

The throughput target alone forces Postgres off the hot path and makes batching mandatory.

1. At 200k to 300k TPS, one-by-one processing is impossible at every stage, so **batching is mandatory**.
2. TigerBeetle handles this range on one cluster. It uses a single core and a single leader by design, and batches of up to 8,190 transfers amortize replication. It is not the bottleneck, but it runs closer to its ceiling than its makers recommend.
3. A single Postgres writer (for example the largest Aurora instance), doing several writes per transaction, is very likely over its comfortable limit. This is an educated guess, not a measurement.
4. If Postgres stays on the hot path, CDC (reading its WAL) is the cheapest way to get data out, because it reuses log writes the database makes anyway. Polling, NOTIFY and read-backs all cost more.
5. CDC at the limit is fragile. Logical decoding adds WAL volume and runs roughly single-threaded per replication slot. A lagging consumer forces Postgres to retain WAL, which can fill disk and stall the database.
6. **Conclusion:** either shard Postgres by request key (easy, since it holds no money invariants), or demote it to an async projection. This design chooses demotion.
7. Kafka becomes the durable intent log, TigerBeetle the durable outcome and invariant enforcer, and Postgres a downstream read model.

## The river architecture

Requests flow through three Kafka topics and one stage, the batcher; Postgres sits downstream as a projection, off the hot path.

&#91;embedded content: the river · 3 topics, 1 stage, 3 consumers\]

With hashed IDs, the edge derives the TigerBeetle ID itself, so no ID-assignment stage is needed. The batcher's replay safety comes from TigerBeetle's idempotency on both successes and failures.

## Correctness under crash-stop

Every crash point resolves to a replay that produces the same outcome, provided the rules below hold.

| Crash point | What happens on replay | Rule that makes it safe |
| --- | --- | --- |
| Stage 1 crashes before its transaction commits | Output is aborted; the record is reprocessed and may get a new ULID | No outside effects in stage 1; downstream reads committed records only |
| Stage 1 sees the same client key twice | The state store returns the first assigned ID | Dedupe map keyed by client key, with a defined time-to-live |
| Stage 2 crashes before calling TigerBeetle | The batch is re-polled and submitted | Offsets committed only after results are written |
| Stage 2 crashes after TigerBeetle, before writing results | Resubmission returns `exists` for successes and `id_already_failed` for transient failures | TigerBeetle idempotency covers both successes and rejections (verified, since 0.16.4) |
| A hold is abandoned (no post, no void) | TigerBeetle voids it after its timeout and returns the funds | Every authorization is a pending transfer with a timeout |
| A result is lost on the way to the client | The client retries with the same key and gets the same outcome | Results are looked up by idempotency key, never recomputed |

Two consequences to design for:

- On replay, stage 2 learns *that* a rejection happened, but not necessarily the original reason (for example insufficient funds versus account not found). Either persist the reason in the result topic before committing, or accept a generic rejection reason on the rare replay path.
- `exists` must be treated exactly like `ok`, and the original result rebuilt by looking the transfer up.

## Facts: verified versus assumed

The TigerBeetle facts below come from its documentation; the card-network and sizing facts are recollections or estimates to verify.

| Claim | Status | Source |
| --- | --- | --- |
| Pending transfers can carry a timeout; on expiry the full amount returns to the original account | Verified | [Two-phase transfers](https://docs.tigerbeetle.com/coding/two-phase-transfers/) |
| Timeout is an interval in seconds, measured from arrival at the primary | Verified | [Time](https://docs.tigerbeetle.com/coding/time/) |
| A transfer ID that failed with a transient error (for example exceeds\_credits) returns `id_already_failed` on retry; a new attempt needs a new ID | Verified, since 0.16.4 | [create\_transfers](https://docs.tigerbeetle.com/reference/requests/create_transfers/) |
| `exists` should be handled exactly like `ok` for crash recovery | Verified | [create\_transfers](https://docs.tigerbeetle.com/reference/requests/create_transfers/) |
| Batches of up to about 8,190 transfers; single core, single leader; more nodes add reliability, not throughput | Verified | [Performance](https://docs.tigerbeetle.com/concepts/performance/) |
| Expiry voids may land a second or two after the timeout | Practitioner report | [Aivars Kalvāns](https://dev.to/aivarsk/running-tigerbeetle-without-a-control-plane-database-part-two-3c0p) |
| Random IDs give about 10% lower throughput than strictly increasing IDs | Verified | TigerBeetle Data Modeling |
| Card-network authorization is synchronous on the hot path, with a few seconds' window | Recollection, verify | Scheme specifications |
| Issuers decide hold expiry, typically about a week for purchases, longer for hotels and car rentals | Typical practice, verify | — |
| Kafka exactly-once commit interval defaults to about 100 ms | Recollection, verify | Kafka Streams docs |
| Dedupe state is roughly 1 to 2 TB per day at 200k TPS | Rough estimate | — |

## Open problems

The biggest open problem is the synchronous edge: getting each result back to the process holding the client's connection, within the deadline.

1. **Sync edge and reply routing.** Options:
   - Key the result topic by the client key with the same partition count as the input, so each answer lands on a predictable partition. Edge instances then forward requests to, or listen on, the matching partition.
   - A reply-to header naming the instance or partition, as in Kafka request-reply.
   - A lightweight signal channel (Redis pub/sub or NATS) carrying only "done" pings.
   - Simplest fallback: every edge instance consumes all results. Chosen to start; see Reply routing below.
2. **Deadline semantics.** The deadline must travel with the command, and the core must refuse to execute expired commands. Proposed: two deadlines plus a compensating void. See Deadlines and failover below.
3. **Two front doors.** An async API (202 plus webhook or status lookup) for capable clients, and a sync adapter for clients who pay for it. Both are thin clients of the same core.
4. **Latency budget.** Two exactly-once commits plus a TigerBeetle round trip per request. Measure p99 against the card-auth window.
5. **Silent TigerBeetle events.** Auto-expired holds produce no Kafka record. Downstream views need TigerBeetle CDC or a periodic scan by timestamp. Newer TigerBeetle versions may offer CDC; verify.
6. **Failover restore time.** A moved partition may need minutes to rebuild RocksDB from the changelog. Use standby replicas and stable group membership, and keep state small.
7. **Dedupe window.** A business decision: how long duplicates are recognized (24 to 72 hours is a starting guess). It sets the state size.
8. **ID time-ordering.** Parked behind one interface; hashing is the default. See ID strategy below.
9. **Stage 2 result rebuild.** The lookup path that reconstructs a result after `exists` or `id_already_failed`.

## Deadlines and failover

The edge answers within 1.5 s, and a "timed out" answer must mean that no money moved.

- **Context:** the ledger replied "timed out" at 1.5 s, inside the bank's 2 to 3 s scheme budget. Those replies did not count toward SLA penalties.
- **No retries within a round** (recollection, verify): on an issuer timeout, the networks apply stand-in rules and later send an advice. The follow-up is a reversal, not a resend. Merchant retries are usually new authorizations with new identifiers. So the timeout answer is final for that round.
- **Two deadlines:** each command carries a core deadline of about 1.2 s. Stage 2 emits "expired" instead of calling TigerBeetle past it. The edge deadline stays at 1.5 s; the gap covers post-TigerBeetle propagation.
- **Compensating void:** when the edge times out, it emits a void for that transfer ID. If the hold exists it is released; if not, the void fails harmlessly. The pending transfer's own timeout is the final backstop.
- **Normal-case budget** (estimate): about 150 to 400 ms with default exactly-once commit intervals, under 100 ms when tuned. The risk is tail events, not the average.
- **Standby replicas:** Kafka Streams keeps warm copies of state on other instances, so failover replays only a small tail. Kafka 4.2's Streams rebalance protocol moves coordination to brokers and makes standbys a group-level setting, but does not yet support static membership.
- **Remaining gap is failure detection,** typically seconds. Partitions owned by a dead instance stall, and their requests time out and are voided.
- **Why not 1 s detection:** pauses and network blips cause false evictions and rebalance storms; detection is only part of the outage; brokers enforce minimum session timeouts; heartbeat traffic grows. Correctness survives anyway, thanks to transactional fencing and TigerBeetle idempotency.
- **Alternative: hedged submission.** A second consumer group submits requests still unanswered after about 300 ms; TigerBeetle returns `exists` for duplicates. This requires deterministic IDs and can double TigerBeetle load in the worst case.
- **Partition count is a blast-radius setting:** more partitions per instance means each failure touches a smaller slice of traffic.

## ID strategy (parked)

The mapping from a client key to a TigerBeetle ID sits behind one interface, so it can be swapped without touching the pipeline.

- **Options:** client-supplied ULIDs, a hash, a Kafka Streams mapping, or a DynamoDB mapping.
- **Default: hashing.** A 128-bit hash of client ID plus client key needs no store, is deterministic, survives replay and enables hedged submission.
- **Cost:** TigerBeetle documents about 10% lower throughput for random IDs than for strictly increasing ones ([Data Modeling](https://docs.tigerbeetle.com/design/data-modeling/)). The likely mechanism (inferred): random keys make LSM files overlap, so compaction rewrites more and existence checks probe more levels.
- **Contract:** TigerBeetle remembers IDs forever, so client keys must be unique per client for all time. Do not scope the hash by calendar date, or a retry crossing midnight becomes a new transfer.
- **Rejected:** offset-derived IDs. They would need the offset of the key's first occurrence, which requires a state store anyway.

**Winning back the \~10%.** If TigerBeetle throughput ever becomes the constraint, switch to time-ordered IDs behind the same interface. Either clients supply ULIDs or TigerBeetle-style time-based IDs, or a mapping layer (a Kafka Streams stage with a state store, or DynamoDB with conditional puts) assigns one on first sight of each client key. Both restore the idempotency guarantee, but the mapping layer brings back the state-store, restore-time and determinism concerns that hashing removed.

## Reply routing

Start with broadcast over a slim replies topic; switch to reply addressing only if the edge fleet grows past about 50 instances.

- **The problem:** the client key decides where work is processed, but the load balancer decides which edge instance holds the TCP or QUIC connection. The result must cross from the first mapping to the second.
- **Best-effort by design:** results are durable in the results topic, and deadlines plus voids keep the ledger consistent. A lost reply is a timeout, not a correctness failure.
- **Edge waiter table:** an in-memory map from request ID to waiting connection, each entry expiring at the deadline. At 200k TPS and 1.5 s, the whole fleet holds at most about 300k entries.
- **Chosen to start: broadcast.** Every edge instance reads a slim replies topic (request ID, outcome code, amount) and keeps only its own entries. The request ID goes in the record key or a header, so non-matching records are skipped without parsing.
- **Cost model:** broker egress = N instances × 200k/s × message size. At about 50 B per reply, that is about 10 MB/s per instance and about 1 GB/s for 100 instances. The lookup itself costs tens of nanoseconds; consumer work is an estimated fraction of a core per instance.
- **Upgrade path:** the edge stamps a reply address on each request. A dispatcher then forwards results either to a per-instance replies partition (stable instance identity, such as a StatefulSet ordinal) or by direct RPC. The switch does not touch the core pipeline.
- **Rejected:** moving the connection to the partition owner. The edge would have to track Kafka assignments through every rebalance.
- **Failures stay benign:** a dead edge instance takes its connections with it, so its replies are discarded; a lagging dispatcher delivers late replies that are dropped.

## Implementation notes (Scala + Cats Effect)

The target is Scala 3 with Cats Effect and fs2 for the shell, around a pure Scala core; Kyo was considered and set aside.

- **Hashing removes stage 1.** With hashed IDs, the edge computes the TigerBeetle ID itself and writes straight to the submission topic. The pipeline becomes edge → submission topic → batcher → TigerBeetle → results and replies topics, with no Kafka Streams dependency.
- **Why Cats Effect over Kyo:** fs2-kafka is the most mature Kafka streaming library in Scala; TestControl is the closest the JVM has to a deterministic scheduler; and http4s or Ember, Skunk or Doobie, and otel4s share one runtime. Kyo has no native Kafka client, and bridging fs2 through kyo-cats would mean two runtimes and a seam for cancellation and backpressure.
- **Trade-off accepted:** Cats Effect's coarser effect typing (mostly `IO`, errors as `Throwable`). Precise typing lives in the pure core as domain types: commands, outcomes, rejection reasons.
- **Stack:** fs2-kafka for producers and consumers; http4s or Ember at the edge; Skunk or Doobie for the Postgres projection; otel4s for tracing.
- **TigerBeetle client:** a thread-safe Java wrapper over the native client, which runs its own internal I/O thread. One shared instance per process lets concurrent events be batched automatically ([Java client docs](https://docs.tigerbeetle.com/coding/clients/java/)). Wrap `createTransfersAsync` (a `CompletableFuture`) with `IO.fromCompletableFuture`; never block on the completion callback.
- **TestControl caveat:** it is deterministic only for pure `IO`. Real fs2-kafka uses the Kafka client's own threads, so TestControl applies to shell tests against simulated ports, not against real Kafka.

## Testing strategy

Testing is the centerpiece of the project: every invariant above is checked by deterministic simulation first, then by real fault injection against real binaries.

| Layer | What runs | Faults | Reproducible | Catches |
| --- | --- | --- | --- | --- |
| 1. Deterministic simulation (DST) | Our code, single process, simulated clock, seeded randomness, simulated Kafka and TigerBeetle | Crash between any two steps, redelivery, duplicates, rebalances, lag, lost replies, slow partitions | Yes, by seed | Interleaving and recovery bugs in pipeline logic |
| 2. Contract tests | The same small suite against simulators and real Kafka and TigerBeetle | None; semantics only | Yes | Simulators drifting from real behavior |
| 3. Network perturbation (Toxiproxy) | Real binaries in Docker Compose or CI, connections routed through proxies | Latency, jitter, bandwidth limits, timeouts, connection resets, half-open connections | Partly, by scripted scenario | Client timeouts, retry storms, deadline handling |
| 4. Cluster chaos (Kubernetes) | Real deployment in a staging cluster | Pod and node kills, network partitions, disk I/O delay, CPU pressure, clock skew | No | Failover, rebalance and restore behavior at realistic scale |
| 5. Optional: deterministic hypervisor (Antithesis) | Real containers in a deterministic environment | Generated faults across the whole system | Yes | Black-box bugs DST cannot model |

**Invariants checked in every layer:**

- No transfer is applied twice.
- Every request reaches exactly one terminal outcome.
- After a "timed out" reply and the void, no hold survives.
- Whenever the edge replied, the reply matches the ledger.
- Balances never break their configured limits.
- Under overload, useful throughput stays above a floor instead of collapsing to zero.

**DST design:** every source of nondeterminism is a port: Kafka producer and consumer, TigerBeetle client, clock, network to the edge, random source. Simulated adapters model real semantics (offsets, commits, redelivery, `exists` and `id_already_failed`, pending timeouts on the simulated clock), not canned mock answers. TigerBeetle's narrow contract makes it easy to model; for Kafka, model only the behavior we depend on.

**Fault tools compared:**

- **Toxiproxy:** a programmable TCP proxy placed between processes. It is free, lightweight and scriptable from tests, so it fits laptops and CI. It only perturbs network connections; it cannot kill processes or skew clocks.
- **Chaos Mesh and LitmusChaos:** open-source, Kubernetes-native tools that define experiments as cluster resources. They cover pod kills, network partitions, I/O faults and stress, and suit scheduled staging runs.
- **Gremlin:** a commercial platform with managed safety controls, aimed at enterprises and production. Probably more than a hobby project needs.
- **Antithesis:** commercial deterministic testing for real containers, with no simulators to write.

**Suggested order:** DST plus contract tests from day one, then Toxiproxy in CI, then Chaos Mesh in a staging cluster once there is a real deployment.

**Core as pure state machines (design decision).** No JVM effect runtime is fully plug-and-play for DST (Kyo lacks a deterministic scheduler; Cats Effect TestControl comes closest but breaks on outside threads), so the money logic is written as a functional core with an imperative shell, as FoundationDB and TigerBeetle do.

- **What Kyo provides:** controllable time via `Clock.withTimeControl` (nested scopes share one timeline since 1.0.0-RC4) and the kyo-test framework ([release notes](https://github.com/getkyo/kyo/releases/tag/v1.0.0-RC4)). A seedable `Random` is likely; verify.
- **What is missing:** a deterministic scheduler. Kyo's runtime is multi-threaded and adaptive, so fiber interleavings vary between runs (inferred; confirm with maintainers). The Kafka and TigerBeetle Java clients also run their own threads.
- **Functional core:** each component (edge waiter logic, batcher, deadline and void rules) is a function from state plus event to new state plus commands. Events include request arrived, batch result returned, time advanced, consumer rebalanced.
- **Simulator:** a plain loop over a priority queue of events ordered by simulated time, with a seeded random generator choosing delays, reorderings and crashes. Fully deterministic; no fibers involved.
- **Imperative shell in Cats Effect:** real I/O, fibers, timeouts and resource safety, wiring the state machines to real Kafka, TigerBeetle and connections.
- **Two test tiers:** state-machine DST with millions of seeded runs checking every invariant; Cats Effect integration tests under TestControl checking that the shell wires effects correctly.
- **Side benefit:** the money logic stays independent of the effect system.

## Alternatives considered

Each alternative below works at moderate scale; none fits 200k TPS with crash-stop correctness as well as the Kafka-first river.

| Alternative | Why not chosen here |
| --- | --- |
| Postgres as hub, CDC to TigerBeetle and back (original design) | Single writer near its limit; CDC slot lag can stall the database |
| Sync HTTP: write Postgres, call TigerBeetle, write result, reply | Fine at about 20k TPS, but needs a separate rarely-exercised sweeper for crashes |
| DynamoDB as idempotency map (conditional put), then TigerBeetle, then Kafka | Adds a third system to the hot path; ordering gap if Kafka is written after TigerBeetle; high cost at peak rates |
| Redis as idempotency map | Weaker durability for a correctness-critical mapping |
| Postgres LISTEN/NOTIFY to wake waiting requests | NOTIFY adds commit contention at high throughput |
| Every edge instance consumes every result | Correct and simple, but N-fold read cost; kept only as a fallback |

When to revisit: below about 20k TPS, the simple sync path plus a sweeper is less machinery and probably the better choice.

## Next steps

Start tomorrow's session with the sync edge, since the async river is settled.

- [ ] Measure consumer CPU and broker egress for broadcast replies
- [ ] Define timeout behavior at the edge: decline, stand-in rules, or pending
- [ ] Decide the dedupe window and estimate state size per partition
- [ ] Check whether TigerBeetle performs well with random ULIDs, or needs offset-derived IDs
- [ ] Check TigerBeetle's current CDC options for observing auto-expired holds
- [ ] Sketch the latency budget per stage against a card-auth window
- [ ] Decide partition counts for input, assigned and result topics
