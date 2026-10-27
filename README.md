# High-Throughput Ledger Pipeline — Design Notes

*As of 2026-10-27 · Dumitru Nicolae Marasoiu*

A hobby design for a card-authorization ledger pipeline that sustains 200,000 transactions per second (TPS), stays correct under crash-stop failures, and is proven correct mainly by deterministic simulation and fault injection. It serves as the starting functional spec, non-functional spec and design for an implementation in Scala 3 with Cats Effect.

**Also in this repo:** [DESIGN-LOG.md](DESIGN-LOG.md) records every decision that was corrected and how the flaw was found; [ROADMAP.md](ROADMAP.md) lists the phases beyond the core.

**Status markers used throughout:** *verified* = checked against the cited source; *estimate* = rough reasoning, to measure; *recollection* or *verify* = believed true, not yet checked.

---

## 1. Context and goals

- **Domain:** card-authorization-style debits. The issuer must answer within a hard deadline; holds (pending amounts) are later posted or voided.
- **Setting:** a hypothetical core-banking platform serving a large bank. Clients may be async-capable or may insist on a synchronous call.
- **Rate:** 200k TPS as an average over a 1-minute window, with 300k TPS as the stress scenario. Sub-second bursts of 2 to 3 times that are absorbed by queues (*estimate*).
- **Deadline:** the ledger answers within **1.5 s**, inside the bank's 2 to 3 s card-network budget. A "timed out" answer is acceptable and does not count toward SLA penalties.
- **Correctness bar:** no double debit, no lost result, a deterministic outcome for every request, and safe cancellation of any request the caller got no definitive answer for, so that a "timed out" reply never leaves money held, even when any process crashes at any point.
- **Building blocks:** Kafka (logs), TigerBeetle (ledger and money invariants), Postgres (queryable projection, off the hot path).

## 2. Design philosophy

Each stage does one clear thing against one clear store, commits its progress, and hands over to the next stage through a log.

1. **One stage, one job, one store.** A failure or overload in one kind of operation does not swamp the others.
2. **Log intent before effect.** Every command is durable in Kafka before it touches the ledger, so every effect can be found and reconciled.
3. **Batch everywhere, idempotent per item.** Throughput comes from batches; safety comes from each item carrying its own idempotency ID.
4. **Nothing random on the path to an effect.** Every ID that reaches TigerBeetle is re-derivable from the request itself, and every decision a replay could make differently, such as expiry by the clock, is written to the ledger instead (section 8).
5. **Recovery is the normal path.** Replay after a crash runs the same code as normal processing.
6. **Keep headroom.** A store at its limit has no room for bursts, failover or a lagging consumer.
7. **Count the couplings.** Three systems at 99.99% each give about 99.97% combined; removing a system beats tuning one.
8. **Functional core, imperative shell.** Money logic is pure state machines; effects live in a thin shell. This is what makes deterministic simulation possible (section 11).

## 3. From 200k TPS to the architecture

The throughput target alone forces Postgres off the hot path and makes batching mandatory.

1. At 200k to 300k TPS, one-by-one processing is impossible at every stage, so **batching is mandatory**.
2. **TigerBeetle should not be the bottleneck; this is the premise to measure first.** It processes batches of up to 8,189 transfers, runs single-core with a single leader by design ([Performance](https://docs.tigerbeetle.com/concepts/performance/)), and is believed to sustain roughly 100k to 500k TPS on one cluster (*recollection, verify*: TigerBeetle's docs give no TPS figure). One third-party cloud benchmark leveled off near 107k TPS on three 4-vCPU replicas with hot accounts, with its batching and ID order unstated ([SoftwareMill](https://softwaremill.com/tigetbeetle-vs-postgresql-performance-benchmark-harness-cloud-tests)). Each authorization also costs TigerBeetle about two events over its life: the hold, then its post, void or expiry. At 200k TPS it runs nearer its ceiling than its makers recommend; if one cluster lacks the headroom, sharding ([ROADMAP](ROADMAP.md) phase B) moves into the core.
3. **A single Postgres writer** doing several writes per transaction is very likely over its comfortable limit (*estimate*).
4. If Postgres stayed on the hot path, CDC from its WAL would be the cheapest way out. But at the limit, CDC is fragile: a lagging consumer forces WAL retention, which can fill disk and stall the database.
5. **Conclusion:** demote Postgres to an async projection. Kafka is the durable intent log; TigerBeetle is the durable outcome and invariant enforcer.

## 4. Architecture: the river

Requests flow through Kafka topics and a single processing stage, the batcher; Postgres sits downstream, off the hot path.

```mermaid
flowchart TD
    Bank["Bank / card network"] <-->|"sync call, 1.5 s deadline"| Edge
    Edge["Edge adapter<br/>hash client key → TigerBeetle ID<br/>waiter table, admission control"]
    Edge -->|"authorization or cancel,<br/>keyed by authorization ID"| Sub[("submission topic")]
    Sub --> Batcher["Batcher<br/>poll, fence late authorizations,<br/>batch up to ~8k"]
    Batcher <-->|"one batch call"| TB[["TigerBeetle<br/>balances, holds, timeouts<br/>idempotent on success and failure"]]
    Batcher --> Res[("results topic<br/>full outcome")]
    Batcher --> Rep[("replies topic<br/>slim outcome")]
    Rep -->|"broadcast; each edge keeps its own"| Edge
    Res --> PG["Postgres projection<br/>batched writes for queries"]
    Res --> Other["Other consumers<br/>fraud, notifications, audit"]
```

- **Edge adapter:** accepts an authorization or a `cancel(clientKey)`, derives the TigerBeetle ID by hashing the client key (section 7), registers a waiter, and writes the command to the submission topic.
- **Submission topic:** keyed by the authorization's transfer ID, so every command about one hold lands on the same partition.
- **Batcher:** polls a chunk, fences authorizations past their core deadline instead of submitting them and reports them `not_executed` (section 8), submits everything to TigerBeetle as one batch, then writes results, slim replies and the consumed offsets in one Kafka transaction. Cancels are never expired: they are always submitted, however late.
- **Replies topic:** broadcast to every edge instance; each completes only its own waiters (section 9).

**Three keys, three jobs.** Idempotency is per command; routing is per hold.

| Key | Identifies | Authorization | Cancel |
| --- | --- | --- | --- |
| Client idempotency key | The client's request | `clientKey` | The same `clientKey` |
| TigerBeetle transfer ID | One ledger command; TigerBeetle deduplicates on it | `A = hash(clientId, clientKey)` | Fence `A` (same ID, no flags) and void `V = hash(A, "void")` |
| Kafka record key | The hold the command concerns; only picks the partition | `A` | `A` |

Every later command about a hold (cancel, capture, reversal) carries `A`, so the partitioner routes every command on `A`. Kafka does not deduplicate on the record key, so two records sharing it are simply two records on one partition, in order. Keying by account would order every operation on an account for no benefit, since TigerBeetle already enforces balances, and would turn busy accounts into hot partitions.

**Submission topic rules** that follow from sharing the key:

- **`cleanup.policy=delete`, never `compact`.** Compaction keeps only the latest record per key, so a cancel would erase its authorization from the intent log, breaking principle 2.
- **Partition count fixed for the topic's life.** The default partitioner maps a key to `hash(key) mod N`; changing N scatters one hold's commands across partitions. Fenced cancels (section 8) keep that correct, but it defeats per-hold locality. Resizing means a new topic and draining the old one, so choose N with headroom.

## 5. Correctness under crash-stop

Every crash point resolves to a replay that produces the same outcome.

| Crash point | What happens on replay | Rule that makes it safe |
| --- | --- | --- |
| Edge crashes before the command is in Kafka | Nothing happened; the caller sees a timeout | Nothing is executed until it is durable in the log |
| Edge crashes after writing to Kafka | The command is processed; the caller sees a connection error and sends `cancel(clientKey)` | The caller's cancel contract and fenced cancels (section 8) |
| Batcher crashes before calling TigerBeetle | The chunk is re-polled and submitted | Offsets are committed in the same transaction as the results |
| Batcher crashes after TigerBeetle, before writing results | Resubmission returns `exists` for successes and `id_already_failed` for transient failures | TigerBeetle idempotency covers both successes and rejections (*verified*, since 0.16.4) |
| Same, but the replay happens after the core deadline | The fence for `A` returns `exists_with_different_flags`, so the authorization was created and the batcher reports it as approved | Expiry is a ledger write, not a clock reading (section 8) |
| A cancel overtakes its authorization, for example a stale send from a crashed edge | The fence creates `A`; the late authorization gets `exists_with_different_flags` and is reported `not_executed` | The authorization ID doubles as a fencing token (section 8) |
| A zombie batcher, paused and then replaced by a rebalance, calls TigerBeetle after its successor has expired the same authorization | The successor's fence holds `A`, so the zombie's authorization gets `exists_with_different_flags` and changes nothing; if the zombie got there first, the fence finds the authorization and the successor reports it approved. Kafka rejects the zombie's results transaction, provided its offsets carry the consumer's live group metadata (section 6) | Kafka's transactional fencing covers Kafka writes, not TigerBeetle calls; the fence covers the ledger |
| The same client key arrives twice | The same hashed ID returns the original outcome. Callers never resend after a non-definitive result, so a duplicate is a stale send or a redelivery; one that lands after its cancel is reported approved, and consumers ignore it | ID derived deterministically from the client key; a cancel is never overridden (section 8) |
| A hold is abandoned | TigerBeetle voids it after its timeout and returns the funds | Every authorization is a pending transfer with a timeout |
| A reply is lost on the way to the client | It is a timeout from the caller's view, so the caller cancels | The caller's cancel contract (section 8) |

**Consequences to design for:**

- On replay, the batcher learns *that* a transient rejection happened, but not necessarily the original reason (for example insufficient funds versus account not found). Persist the reason in the result before committing, or accept a generic reason on the rare replay path.
- `exists` must be treated exactly like `created` (called `ok` before TigerBeetle 0.17.0). Since 0.17.0 `exists` returns the original timestamp, and its fields match the request, so the original result is rebuilt without a lookup. It says the authorization was created, not that its hold is still active: a later void does not change it (section 12, follow-up 7).
- **One outcome for a fenced authorization.** An authorization whose ID is fenced is reported `not_executed`, whether the fence came from expiry or from a cancel. Separate `expired` and `cancelled` outcomes would let one request end with two: a retried copy of an expired authorization would find the fence and be reported `cancelled`. Why it was fenced is recorded with the fence itself. A cancel request has its own outcome, `cancelled`.
- On an authorization, `exists_with_different_flags` means a fence: TigerBeetle compares flags before any other field, the edge always sets `pending`, and a fence never does. A reused client key with a different payload shows up as another `exists_with_different_*` code instead. No lookup is needed to tell them apart.
- On a fence, only `exists_with_different_flags` means the authorization holds `A`. `exists`, or another `exists_with_different_*` code, means an earlier fence holds it, for example when a cancel's fence meets an expiry fence that records a different reason: the authorization never happened.

## 6. Facts: verified versus assumed

| Claim | Status | Source |
| --- | --- | --- |
| Pending transfers can carry a timeout; on expiry the full amount returns | Verified | [Two-phase transfers](https://docs.tigerbeetle.com/coding/two-phase-transfers/) |
| Timeout is an interval in seconds from arrival at the primary | Verified | [Time](https://docs.tigerbeetle.com/coding/time/) |
| A transfer ID that failed with a transient error returns `id_already_failed` on retry; a new attempt needs a new ID | Verified, since 0.16.4 | [create_transfers](https://docs.tigerbeetle.com/reference/requests/create_transfers/) |
| `pending_transfer_not_found` is one of those transient errors | Verified | [create_transfers](https://docs.tigerbeetle.com/reference/requests/create_transfers/) |
| `pending_transfer_expired` is not transient | Verified | [create_transfers](https://docs.tigerbeetle.com/reference/requests/create_transfers/) |
| Events in one batch run in a sequential loop, and later events see earlier ones' effects (a transient failure burns an ID for the rest of the same batch) | Verified in source | [source](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine.zig), [tests](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine_tests.zig) |
| A void may reference a pending transfer created earlier in the same batch | Verified in source and upstream tests | [tests](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine_tests.zig) (`create_transfers: closing accounts`) |
| The ID existence check runs before any other validation: an existing ID returns `exists` or `exists_with_different_*`, an orphaned one `id_already_failed` | Verified in source | [source](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine.zig) (`create_transfer`) |
| `exists_with_different_*` compares flags first, so a pending authorization against a non-pending fence returns `exists_with_different_flags` | Verified in source | [source](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine.zig) (`create_transfer_exists`) |
| After flags, `pending_id` and `timeout` are compared before accounts, amount, user data, ledger and code, so a field the edge derives from configuration, such as the hold timeout, is part of a retry's identity | Verified in source | [source](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine.zig) (`create_transfer_exists`) |
| A voided or expired authorization still returns `exists`: the comparison never reads the hold's pending status | Verified in source | [source](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine.zig) (`create_transfer_exists`) |
| Exactly seven codes are transient and burn the ID: `debit_account_not_found`, `credit_account_not_found`, `pending_transfer_not_found`, `exceeds_credits`, `exceeds_debits`, `debit_account_already_closed`, `credit_account_already_closed`. A fence that fails with one still burns `A` | Verified in source | [source](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/tigerbeetle.zig) (`CreateTransferStatus.transient`) |
| Zero-amount transfers are allowed (since 0.16.0) and leave account balances untouched | Verified in source | [source](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine.zig), [tests](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine_tests.zig), CHANGELOG |
| Voiding a non-pending transfer returns `pending_transfer_not_pending`, which is not transient | Verified in source | [source](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine.zig) (`post_or_void_pending_transfer`) |
| A void past the pending transfer's timeout returns `pending_transfer_expired` even before the expiry pulse has run | Verified in source | [source](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine.zig) (`post_or_void_pending_transfer`) |
| `exists` should be handled exactly like `created` for crash recovery | Verified | [create_transfers](https://docs.tigerbeetle.com/reference/requests/create_transfers/) |
| Since 0.17.0 every event gets a status and a timestamp; `ok` is renamed `created`, and `exists` returns the original timestamp | Verified | [API changes](https://docs.tigerbeetle.com/coding/api-changes/), [source](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/tigerbeetle.zig) |
| Batches of up to 8,189 transfers; single core, single leader | Verified | [Requests](https://docs.tigerbeetle.com/coding/requests/), [Performance](https://docs.tigerbeetle.com/concepts/performance/) |
| The built-in `tigerbeetle benchmark` can model this workload: pending transfers, random IDs, Zipfian account choice, several clients | Verified in source | [source](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/tigerbeetle/cli.zig) (`Benchmark`) |
| Random IDs give about 10% lower throughput than strictly increasing IDs | Verified | [Data modeling](https://docs.tigerbeetle.com/design/data-modeling/) |
| Java client is thread-safe; one shared instance batches concurrent events | Verified | [Java client](https://docs.tigerbeetle.com/coding/clients/java/) |
| Expiry runs in a `pulse` that the primary injects after a commit, or every 100 ms when idle; each pulse expires up to one batch (8,189), so lag is about 100 ms unless expiries back up. A practitioner observed a second or two | Verified in source; measure in the contract suite | [source](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/vsr/replica.zig) (`pulse_timeout`, `commit_execute`), [Aivars Kalvāns](https://dev.to/aivarsk/running-tigerbeetle-without-a-control-plane-database-part-two-3c0p) |
| Built-in change data capture reports every transfer, including auto-expired holds as `two_phase_expired`, but publishes only over AMQP 0.9.1 (RabbitMQ), at least once, from a single instance. The `get_change_events` operation underneath is not in the Java client | Verified | [CDC](https://docs.tigerbeetle.com/operating/cdc/), [source](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/tigerbeetle.zig) |
| Kafka's default partitioner maps a key to `murmur2(key) mod N` | Verified in source | [source](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java) (`partitionForKey`) |
| A zombie's transactional offset commit is rejected once its partition is reassigned, under both the classic and the new consumer group protocols, but only if the producer sends the consumer's live group metadata; a commit carrying only the group ID is not checked | Verified in source | [source](https://github.com/apache/kafka/blob/trunk/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/modern/consumer/ConsumerGroup.java) (`validateOffsetCommit`) |
| fs2-kafka sends the consumer's live group metadata with transactional offsets | Verified in source | [source](https://github.com/fd4s/fs2-kafka/blob/main/modules/core/src/main/scala/fs2/kafka/KafkaProducer.scala) |
| Card networks do not resend an authorization within a round; they apply stand-in and send an advice | Recollection, verify | Scheme specifications |
| Card-network authorization is synchronous on the hot path | Recollection, verify | Scheme specifications |
| An acquirer that gets no issuer answer sends a reversal for that authorization | Recollection, verify | ISO 8583 and scheme specifications |
| A reversal echoes the original authorization's identifiers (ISO 8583 field 90, original data elements) | Recollection, verify | ISO 8583 and scheme specifications |

## 7. ID strategy

The mapping from a client key to a TigerBeetle ID sits behind one interface, so it can be swapped without touching the pipeline.

- **Chosen: hashing.** The ID is a 128-bit hash of client ID plus client key. It needs no store, is deterministic, survives replay, and lets the edge compute it before anything else happens.
- **Cost:** about 10% lower TigerBeetle throughput than strictly increasing IDs (*verified*). The likely mechanism (*inferred*): random keys make LSM files overlap, so compaction rewrites more and existence checks probe more levels.
- **Contract:** TigerBeetle remembers IDs forever, so client keys must be unique per client for all time. Do not scope the hash by calendar date, or a retry crossing midnight becomes a new transfer.
- **The hash is permanent.** A cancel recomputes `A` from the client key, so changing the hash function or how its inputs are encoded would aim the cancels of existing holds at the wrong ID. Fix it once in stage 1: a cryptographic hash truncated to 128 bits, over length-prefixed fields with a tag per ID kind, so no client key can encode to the input of a void ID, and never 0 or the maximum value, which TigerBeetle rejects.
- **Retry contract:** once an ID fails with a transient error or is voided, it can never succeed again. (Validation errors are not recorded, so an ID rejected for those could succeed if resent with corrected fields; with hashed IDs and unchanged fields this does not arise.) A client never resends an authorization after a non-definitive result: it cancels, and a genuinely new attempt uses a new key (section 8). Reusing a key is left to the system itself, when a batcher replays after a crash or the network duplicates a send.
- **Cancel IDs:** a cancel's fence reuses `A` itself, and its void gets the fixed ID `V = hash(A, "void")`, so cancels are idempotent too.
- **Winning back the ~10%:** switch to time-ordered IDs behind the same interface. Either clients supply ULIDs or TigerBeetle-style time-based IDs, or a mapping layer assigns one on first sight of each client key (a Kafka Streams stage with a state store, or DynamoDB with conditional puts). The mapping layer brings back a state store, restore time after failover, and roughly 1 to 2 TB of dedupe state per day at 200k TPS (*estimate*), which is why it is not the default.
- **Rejected:** IDs derived from Kafka partition and offset. They would need the offset of the key's first occurrence, which requires a state store anyway.

## 8. Deadlines, cancels and failover

A "timed out" answer must mean that no hold survives, and any request without a definitive answer must be safely cancellable.

**Deadlines**

- **No retries within a round** (*recollection, verify*): on an issuer timeout, the networks apply stand-in rules and later send an advice. The follow-up is a reversal, not a resend; merchant retries are usually new authorizations with new identifiers. So the timeout answer is final for that round.
- **Two deadlines:** each command carries a core deadline of about 1.2 s, stamped as absolute time at the edge. The batcher fences authorizations past it instead of submitting them (replay-safe expiry, below); cancels are exempt. The edge deadline stays at 1.5 s; the gap covers post-TigerBeetle propagation.
- **Clocks:** the core deadline is stamped on one machine and checked on another. Normal clock synchronization keeps drift to milliseconds; this assumption is part of the design.
- **Normal-case budget** (*estimate*): about 150 to 400 ms, dominated by the batcher's transaction cadence and the edge's read-committed consumption of replies; under 100 ms when tuned. The risk is tail events, not the average.

**Fencing in plain terms**

*The problem.* A cancel must undo an authorization, but nothing guarantees the authorization reaches TigerBeetle first:

1. Edge instance E1 sends authorization `A` toward Kafka, then crashes before replying.
2. The caller gets no answer and sends a cancel through edge instance E2.
3. The cancel reaches TigerBeetle first. There is nothing to void yet, so the void does nothing.
4. E1's delayed authorization arrives, from a network buffer or a retry, and creates the hold.

The caller believes the request is cancelled, while the customer's money is held, possibly for days. A plain void cannot help: it acts only on something that already exists.

*The idea.* TigerBeetle never allows two transfers with the same ID. So the cancel first takes the authorization's own ID for itself, which makes it impossible for the authorization ever to be created afterwards. It is like putting your coat on a theatre seat when you cannot tell whether your friend has already sat down. If the friend is there, the coat does not fit, so you ask them to leave: that is the void. If the seat is empty, the coat takes it, and a friend arriving later finds it taken for good. Either way, nobody ends up in the seat.

*The mechanics.* A cancel sends two operations: a **fence**, a dummy zero-amount transfer between two internal accounts that carries the authorization's ID `A`, and a **void** of whatever hold has ID `A`. If the authorization came first, the fence fails because `A` is taken, and the void releases the hold. If the cancel came first, the fence takes `A`, the void finds nothing, and the late authorization is rejected because `A` is taken. The rejection is recognizable: TigerBeetle reports that `A` exists with different flags, which happens only when a fence got there first, so the batcher reports it `not_executed`.

*Why "fencing".* In distributed systems, a fencing token stops a stale actor, such as a crashed process's leftover message, from acting after someone else has taken over. Here the authorization ID is the token: once the cancel holds it, the stale authorization is locked out.

*The same trick fixes expiry.* A batcher that decides "expired" from the clock can be wrong after a crash and replay, because the authorization may have gone through before the crash. Writing the fence instead asks the ledger: if the fence takes `A`, the authorization never happened and never will; if `A` is already taken, it went through, and the batcher reports it as approved.

*The cost.* One extra zero-amount transfer per cancel or expiry, both rare; two internal fence accounts set up once; and every fenced ID stays in TigerBeetle forever, as every ID already does.

**Cancels: fence, then void**

A cancel either releases what an authorization did or stops it from ever happening, whichever order they reach TigerBeetle in. It is one TigerBeetle batch of two unlinked events:

1. **Fence:** ID `A` itself, amount 0, no flags, a dedicated fence `code`, between two fence accounts on a fence ledger. Zero amount leaves both accounts untouched. One pair of fence accounts serves every currency, because TigerBeetle checks the ID before it checks ledgers.
2. **Void:** ID `V = hash(A, "void")`, `pending_id = A`, amount 0 (the full pending amount), and accounts, ledger and code left at 0 to inherit from the pending transfer.

The two events must not be linked: the fence is expected to fail when the authorization got there first, and a linked chain would roll the void back with it. The fence goes first in the batch.

| What reaches TigerBeetle first | Fence | Void | Late authorization |
| --- | --- | --- | --- |
| The authorization | `exists_with_different_flags` | Releases the hold, or `pending_transfer_expired` if it already timed out | None |
| The authorization, which failed with a transient error | `id_already_failed` | `pending_transfer_not_found`; nothing was ever held | None |
| The cancel | Created | Fails harmlessly with `pending_transfer_not_pending` | `exists_with_different_flags`, reported `not_executed` |

A replayed cancel is idempotent in outcome, though not in every result code: a fence or void that was created now returns `exists`, and a void that failed with `pending_transfer_not_found` now returns `id_already_failed`, because that transient failure burned `V`. No rule about partitions, producers or batch boundaries is needed for correctness, so a stale authorization from a crashed edge cannot bring a cancelled hold back.

**Who cancels**

1. **The caller (the guarantee).** Any non-definitive result, whether a "timed out" reply, a connection error or no answer, obliges the caller to send `cancel(clientKey)`, retried until acknowledged. Cancelling is the only way forward: the caller never resends the authorization with the same key, the acknowledgement is final, and a new attempt uses a new key. A resend could not be trusted anyway, since TigerBeetle returns `exists` for a voided authorization and the edge may already be cancelling. This mirrors the acquirer's timeout reversal in card payments (*recollection, verify*). The caller is the bank's own authorization system, so the contract is enforceable.
2. **The edge (an optimization).** On its 1.5 s timeout the edge replies "timed out" at once and writes the cancel itself, keeping holds short even when the caller is slow. It is the same command the caller sends, with the same fence, void IDs and fence reason, so whichever lands second changes nothing. If the edge crashes, the caller's cancel is the guarantee.
3. **Backstops.** Network reversals (section 12, gap 3) and the pending transfer's own timeout. That timeout cannot be short, since a real hold must last until capture.

**Why not a ledger-side sweeper:** the ledger cannot tell "the caller never got an answer" from "the reply was written but lost". A sweeper would sometimes void a hold the caller believes approved, which is worse than an orphaned hold.

**No stand-in exception:** a timed-out authorization is always cancelled, even when the bank approved on the ledger's behalf. The stand-in becomes a new forced hold with its own ID derived from `A` (section 12, gap 3). Skipping the cancel would not keep the hold anyway: the edge cancels on its own timeout, and if neither cancelled and `A` had gone through, `A`'s hold and the forced hold would hold the money twice.

**Replay-safe expiry**

Expiry by clock reading is not replay-safe. A batcher that submits `A` at 1.1 s, crashes before committing, and replays at 1.3 s would report the authorization as not executed while TigerBeetle holds the funds; the edge would then tell the caller "declined" and send no cancel. So expiring an authorization means writing its fence. If the fence returns `exists_with_different_flags`, the authorization had already gone through and the batcher reports it as approved; `id_already_failed` means it had been rejected, with the reason unknown on this path (section 5); any other `exists` result means a cancel fenced it first, so it is `not_executed`. The cost is one TigerBeetle event per expired request. Expiries are rare, and under overload Kafka and the batcher saturate before TigerBeetle.

**Failover**

- **Batcher state is small:** with hashed IDs the batcher keeps no dedupe store, so failover means reassigning partitions and resuming from committed offsets. There is no state to restore.
- **Remaining gap is failure detection,** typically seconds, governed by the session timeout. Partitions owned by a dead instance stall; their requests time out and are cancelled.
- **Why not 1 s detection:** pauses and network blips cause false evictions and rebalance storms; detection is only part of the outage; brokers enforce minimum session timeouts; heartbeat traffic grows. Correctness would survive anyway: Kafka's transactional fencing stops a zombie batcher's Kafka writes, and TigerBeetle idempotency plus the fence make its TigerBeetle calls harmless (section 5).
- **Partition count is a blast-radius setting:** more partitions per instance means each failure touches a smaller slice of traffic.
- **Deferred: hedged submission.** A second consumer group submitting requests still unanswered after about 300 ms would remove the failover stall. Fenced cancels make it safe, but it can double TigerBeetle load.

## 9. Reply routing

Start with broadcast over a slim replies topic; switch to reply addressing only if the edge fleet grows past about 50 instances.

- **The problem:** the authorization ID decides where work is processed, but the load balancer decides which edge instance holds the TCP or QUIC connection. The result must cross from one mapping to the other.
- **Best-effort by design:** results are durable, and deadlines plus cancels keep the ledger consistent. A lost reply is a timeout, not a correctness failure.
- **Edge waiter table:** an in-memory map from request ID to waiting connection, each entry expiring at the deadline. The whole fleet holds at most about 300k entries at 200k TPS and 1.5 s.
- **Chosen: broadcast.** Every edge instance reads the slim replies topic (request ID, outcome code, amount) and completes only its own waiters. The request ID sits in the record key or a header, so non-matching records are skipped without parsing.
- **Cost model:** broker egress = N instances × 200k/s × message size. At about 50 B per reply, that is about 10 MB/s per instance and about 1 GB/s for 100 instances. The lookup costs tens of nanoseconds; consumer work is an estimated fraction of a core per instance.
- **Upgrade path:** the edge stamps a reply address on each request, and a dispatcher forwards results to a per-instance replies partition (stable instance identity, such as a StatefulSet ordinal) or by direct RPC. The switch does not touch the core pipeline.
- **Rejected:** moving the connection to the partition owner. The edge would have to track Kafka assignments through every rebalance.
- **Failures stay benign:** a dead edge instance takes its connections with it, so its replies are discarded; a lagging consumer delivers late replies that are dropped.

## 10. Implementation notes (Scala 3 + Cats Effect)

The shell uses Cats Effect and fs2 around a pure Scala core; Kyo was considered and set aside.

- **Why Cats Effect over Kyo:** fs2-kafka is the most mature Kafka streaming library in Scala; TestControl is the closest the JVM has to a deterministic scheduler; http4s or Ember, Skunk or Doobie, and otel4s share one runtime. Kyo has no native Kafka client, and bridging fs2 through kyo-cats would mean two runtimes and a seam for cancellation and backpressure.
- **Trade-off accepted:** coarser effect typing (mostly `IO`, errors as `Throwable`). Precise typing lives in the pure core as domain types: commands, outcomes, rejection reasons.
- **Stack:** fs2-kafka for producers and consumers; http4s or Ember at the edge; Skunk or Doobie for the Postgres projection; otel4s for tracing.
- **TigerBeetle client:** a thread-safe Java wrapper over the native client, which runs its own internal I/O thread. Use one shared instance per process. Wrap `createTransfersAsync` (a `CompletableFuture`) with `IO.fromCompletableFuture`; never block on the completion callback.
- **TestControl caveat:** it is deterministic only for pure `IO`. Real fs2-kafka uses the Kafka client's own threads, so TestControl applies to shell tests against simulated ports, not against real Kafka.

## 11. Testing strategy

Testing is the centerpiece of the project: every invariant is checked by deterministic simulation first, then by fault injection against real binaries.

| Layer | What runs | Faults | Reproducible | Catches |
| --- | --- | --- | --- | --- |
| 1. Deterministic simulation (DST) | Pure core plus simulated Kafka, TigerBeetle, clock and network in one thread | Crash between any two steps, redelivery, duplicates, rebalances, lag, lost replies, slow partitions, overload | Yes, by seed | Interleaving and recovery bugs in pipeline logic |
| 2. Contract tests | The same small suite against simulators and real Kafka and TigerBeetle | None; semantics only | Yes | Simulators drifting from real behavior |
| 3. Shell tests (TestControl) | Cats Effect shell against simulated ports | Timeouts, cancellation, scheduling order | Mostly, by seed | Wiring bugs in the shell |
| 4. Network perturbation (Toxiproxy) | Real binaries in Docker Compose or CI | Latency, jitter, bandwidth limits, resets, half-open connections | Partly, by scripted scenario | Timeouts, retry storms, deadline handling |
| 5. Cluster chaos (Chaos Mesh or LitmusChaos) | Real deployment in a staging cluster | Pod and node kills, partitions, I/O delay, CPU pressure, clock skew | No | Failover and rebalance behavior at scale |
| 6. Optional: Antithesis | Real containers in a deterministic hypervisor | Generated faults across the whole system | Yes | Black-box bugs DST cannot model |

**Invariants checked in every layer:**

- No transfer is applied twice.
- Every request reaches exactly one terminal outcome.
- Once a cancel for a request has been processed, no hold for that request survives, and no later authorization with its ID can create one.
- Whenever the edge replied with an outcome, it matches the ledger.
- Balances never break their configured limits.
- Under overload, useful throughput stays above a floor instead of collapsing to zero.

**Core as pure state machines.** No JVM effect runtime is fully plug-and-play for DST: Kyo lacks a deterministic scheduler, and Cats Effect TestControl breaks on outside threads. So the money logic is a functional core with an imperative shell, as FoundationDB and TigerBeetle do.

- **Functional core:** each component (edge waiter and admission logic, batcher, deadline, fence and cancel rules) is a function from state plus event to new state plus commands. Events include request arrived, batch result returned, time advanced, consumer rebalanced.
- **Two interpreters for the commands:** the Cats Effect shell performs them for real; the simulator performs them in a deterministic event loop.
- **Simulator:** a plain loop over a priority queue of events ordered by simulated time, with a seeded random generator choosing delays, reorderings and crashes. No fibers involved.
- **Simulated adapters model real semantics,** not canned mock answers: offsets, commits, redelivery and rebalances for Kafka; `exists`, `id_already_failed`, `exists_with_different_flags` and the other `exists_with_different_*` codes, `pending_transfer_not_pending`, `pending_transfer_expired`, balance limits and pending timeouts on the simulated clock for TigerBeetle.
- **Reference design worth studying:** Haskell's [io-sim](https://hackage.haskell.org/package/io-sim), which runs the same code under real IO or a simulator with schedule exploration.

**Fault tools compared:**

- **Toxiproxy:** a programmable TCP proxy between processes. Free, lightweight and scriptable from tests; fits laptops and CI. It only perturbs connections; it cannot kill processes or skew clocks.
- **Chaos Mesh and LitmusChaos:** open-source, Kubernetes-native; experiments are cluster resources covering pod kills, partitions, I/O faults and stress. Suited to scheduled staging runs.
- **Gremlin:** commercial, with managed safety controls; aimed at enterprises and production.
- **Antithesis:** commercial deterministic testing for real containers, with no simulators to write.

**Suggested order:** DST and contract tests from day one, then Toxiproxy in CI, then Chaos Mesh in a staging cluster once there is a real deployment.

## 12. Known gaps

These are open in the functional and non-functional spec.

1. **Overload and admission control.** With hard deadlines, a backlog makes every queued request expire, and useful throughput drops toward zero while the system looks busy. The edge must decline fast once lag or the in-flight count crosses a threshold; the batcher's cheap fencing of late authorizations is the second line of defense.
2. **What an authorization checks.** TigerBeetle enforces balances. Card status, limits, currency and blocked accounts are unspecified, as is whether they run before TigerBeetle or are encoded as TigerBeetle accounts and limits.
3. **The rest of the card lifecycle.** Capture (including partial captures), incremental authorizations, reversals, and stand-in advices that force a post or void after the fact. Network reversals and advices arrive with network identifiers, not `A`. A lookup from network identifiers to `A`, served by the results topic or the Postgres projection, would bring back the ordering race: a reversal can arrive before its authorization's result is visible. Prefer deriving the client key from identifiers the reversal echoes (section 6), so the caller computes `A` directly, with no lookup.
4. **Silent TigerBeetle events.** Auto-expired holds produce no Kafka record. TigerBeetle's built-in change data capture reports them as `two_phase_expired`, but only over AMQP (section 6), so downstream views need a bridge to Kafka or a periodic scan by timestamp. Holds last days, so a scan is probably enough (*estimate*).
5. **Observability.** Per-stage latency, consumer lag, timeout rate and cancel rate from day one.

**Follow-ups from review** (need discussion or deeper analysis):

6. **Same client key, different payload.** A retry that reuses a key with a different amount or account gets TigerBeetle's `exists_with_different_*` errors rather than the original outcome. Define the client-facing response and whether the edge should catch it earlier. The batcher already tells this case apart from a fence by result code alone (section 5). Since callers never resend a key (section 8) and replays carry the fields the edge stamped, such a mismatch means a caller bug; a configuration change, such as a new hold timeout, cannot cause it.
7. **Same-key authorizations around a cancel.** *Resolved* (section 8, who cancels): after a non-definitive result the caller only cancels, the acknowledgement is final, and a new attempt uses a new key. The remaining duplicates are stale sends and redeliveries. One that lands after its cancel is still reported approved, because a voided authorization returns `exists` (section 6), so consumers fold each hold's outcomes and never let a cancel be overridden. Looking up `V` on `exists` stays optional hardening.
8. **Invariants over decisions, not reasons.** The rejection reason does not survive replay (section 5), and a validation rejection is not recorded, so after a cancel's fence a duplicate of a `rejected` authorization comes back `not_executed`. State "exactly one terminal outcome" over the decision, approved or declined, and treat the reason as best-effort detail.

## 13. Alternatives considered

| Alternative | Why not chosen |
| --- | --- |
| Postgres as hub, CDC to TigerBeetle and back (original design) | Single writer near its limit; CDC slot lag can stall the database |
| Sync HTTP: write Postgres, call TigerBeetle, write result, reply | Fine at about 20k TPS, but needs a separate, rarely exercised crash sweeper |
| DynamoDB idempotency map, then TigerBeetle, then Kafka | A third system on the hot path; an ordering gap if Kafka is written after TigerBeetle; high cost at peak |
| Kafka Streams stage assigning time-ordered IDs | State store, restore time and dedupe-window sizing; kept only as the way to win back TigerBeetle's ~10% |
| Redis as idempotency map | Weaker durability for a correctness-critical mapping |
| Postgres LISTEN/NOTIFY to wake waiting requests | NOTIFY adds commit contention at high throughput |
| Reply addressing (per-instance partitions or direct RPC) | Not needed below about 50 edge instances; kept as the upgrade path from broadcast |
| Hedged submission via a second consumer group | Safe with fenced cancels, but can double TigerBeetle load; deferred |
| Ordered voids instead of fenced cancels (same partition, same producer, void after the authorization's send completes) | Breaks when a crashed edge's stale send lands after a cancel from another instance |
| Ledger-side sweeper for unanswered requests | Cannot tell a lost reply from a delivered one; would void holds the caller believes approved |
| Kyo instead of Cats Effect for the shell | No native Kafka client; fs2 bridging means two runtimes |

Below about 20k TPS, the simple sync path plus a sweeper is less machinery and probably the better choice.

## 14. Development stages

Each stage checks the assumptions the next one builds on: the simulator encodes TigerBeetle and Kafka behavior, so that behavior is checked against the real systems first. Work is tracked as one [GitHub milestone](https://github.com/nmarasoiu/dst-200ktps-ledger/milestones) per stage, with issues under each.

| Stage | Goal | Done when |
| --- | --- | --- |
| 0. Fact spike | Source reading and throwaway scripts against single-node TigerBeetle and local Kafka. The fence, expiry, CDC and Kafka fencing facts are verified in source; open is the throughput figure, a run of `tigerbeetle benchmark` on target-class hardware. Card-network behavior needs scheme specifications and blocks only the card lifecycle | Every TigerBeetle and Kafka fact in section 6 is verified or refuted |
| 1. Spec as code | Domain types (commands, outcomes, rejection reasons, routing key) and the invariants of section 11 as predicates over an event history; scope is authorization and cancel only | Invariants compile and fail on hand-written bad histories |
| 2. Pure core | State machines for the batcher, edge waiter and admission control, fenced cancels and replay-safe expiry; no effects | Property tests pass per machine |
| 3. Deterministic simulator | Seeded event loop with simulated Kafka and TigerBeetle that behave as stage 0 found | Millions of seeds pass, failures replay by seed, and planted bugs are caught |
| 4. Contract tests | One suite run against both the simulators and the real binaries | Green on both in CI |
| 5. Cats Effect shell | fs2-kafka, the TigerBeetle client wrapped in `IO`, the http4s edge; TestControl shell tests | End to end on a laptop at low rate, surviving manual kills of any process |
| 6. Offline checker and Toxiproxy | A tool that reads the topics and TigerBeetle after a run and checks the same invariants; network fault scenarios in CI | Scripted network faults pass the checker |
| 7. Performance | Load generator stepping 20k, 100k, 200k and 300k TPS; per-stage latency, broadcast egress, partition counts, admission control under real load | 200k TPS with p99 inside 1.5 s, or a known ceiling and its cause |
| 8. Cluster chaos | Chaos Mesh in a Kubernetes staging cluster; Antithesis optional | The checker stays green across scheduled chaos runs |

- **One set of invariants for every stage.** They are defined once in stage 1 and checked by the simulator, the contract tests, the offline checker and the chaos runs.
- **Modules mirror the stages:** `core` (no dependencies), `sim`, `checker`, `shell`, `contract-tests`, `loadgen`. The compiler keeps the core free of effects.
- **A simulator that never fails proves nothing.** Stage 3 includes planted bugs, such as expiring by the clock without a fence, or a cancel that voids without fencing, and the simulator must catch them.
- **Stages 2 to 4 are the heart of the project.** A walking skeleton on real binaries before stage 5 would mostly repeat stage 0.

Beyond stage 8, [ROADMAP.md](ROADMAP.md) covers the card lifecycle, sharding, formal verification, multiple currencies, accounting views, risk checks, operations and a reusable simulation library.
