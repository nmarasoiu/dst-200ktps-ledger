# Design log

Every decision in the [design notes](README.md) that was later corrected, what was wrong with it, and how the flaw was found. Most were found by one of four methods: re-reading the design against its own invariants, walking a concrete failure scenario step by step, checking a claim against its cited source, or reading the dependency's source code.

| # | Before | After | How the flaw was found | Commit |
| --- | --- | --- | --- | --- |
| 1 | Postgres as the hub, with CDC to TigerBeetle and back | Kafka as the intent log, TigerBeetle as the outcome store, Postgres as an async projection | Throughput arithmetic: one Postgres writer doing several writes per transaction at 200k TPS, with CDC slot lag able to stall the database | [88e1ed4] |
| 2 | A Kafka Streams stage assigning time-ordered IDs, with 1 to 2 TB per day of dedupe state | The edge hashes the client key into the TigerBeetle ID; no ID stage and no state store | Cost of the state store and its restore time after failover, against a documented 10% throughput cost of random IDs | [f38b1ba] |
| 3 | Crash table and next steps still describing the removed ID stage | Rewritten for hashed IDs | Re-reading the whole document for consistency after the change | [f38b1ba] |
| 4 | TigerBeetle "100k to 500k TPS" and in-batch ordering marked *verified* | Downgraded to *verify* | Opening the cited pages: neither states it | [5eec5c5] |
| 5 | The batcher silently dropped commands past their deadline, voids included | Late authorizations get an outcome; voids are never expired | Checking the batcher against the invariant "every request reaches exactly one terminal outcome", and asking what happens to a late void | [5eec5c5] |
| 6 | Results written in a Kafka transaction, offsets committed afterwards | Offsets committed inside the same transaction | Reading the batcher step by step against Kafka's transactional API | [5eec5c5] |
| 7 | Kafka Streams facts (standby replicas, commit interval) after Kafka Streams was dropped | Removed, and the latency budget restated in terms of the actual stages | Re-reading the facts table against the current architecture | [5eec5c5] |
| 8 | Can an authorization and its void share a key, given their idempotency keys differ? | Three keys with three jobs: the client key, the TigerBeetle ID (per command) and the Kafka record key (per hold). The submission topic must be delete-only, with a fixed partition count | A design question about ordering; separating idempotency from routing exposed the compaction and repartitioning hazards | [6fb994a] |
| 9 | Ordered voids: same partition, same producer, void written only after the authorization's send completes | Fenced cancels: write a fence transfer with the authorization's own ID, then void | Walking the edge-crash scenario: a crashed edge's stale send can reach Kafka after a cancel sent through another edge instance, and the void then burns its ID | [ce4e8d6] |
| 10 | A ledger-side sweeper for orphaned holds | The caller cancels on any non-definitive result, like an acquirer's timeout reversal | The ledger cannot tell a lost reply from a delivered one, so a sweeper would void holds the caller believes approved | [ce4e8d6] |
| 11 | Expiry decided by the batcher's clock | Expiry writes the fence, so the ledger decides | Walking a batcher crash: submit at 1.1 s, crash before commit, replay at 1.3 s reports "expired" while the funds are held | [ce4e8d6] |
| 12 | Hedged submission rejected because it breaks void ordering | Safe with fenced cancels; deferred only for the extra TigerBeetle load | Follows from #9 | [ce4e8d6] |
| 13 | Fence behavior assumed from documentation and memory | Verified in TigerBeetle's source: the ID check runs first, flags are compared first, zero-amount transfers are allowed. The fence and the void must not be linked, and no lookup is needed to recognize a fence | Reading `create_transfer`, `create_transfer_exists` and `post_or_void_pending_transfer` in the TigerBeetle source | [6c82771] |
| 14 | Separate `expired` and `cancelled` outcomes for a fenced authorization | One outcome, `not_executed` | Re-reading against the invariants: a retried copy of an expired authorization would find the fence and be reported `cancelled`, giving one request two outcomes | [06eced4] |
| 15 | Correctness after a rebalance credited to Kafka's transactional fencing and TigerBeetle idempotency | Kafka's fencing stops only a zombie batcher's Kafka writes; the fence makes its TigerBeetle calls harmless. Added to the crash table | Listing every actor that can create `A` against every actor that decides `A` must never exist | This change |
| 16 | Stand-in exception: the caller must not cancel after a stand-in | No exception: the timed-out authorization is always cancelled, and the stand-in is a new forced hold with its own ID | The edge cancels on its own timeout anyway; and if neither cancels and `A` went through, `A`'s hold and the forced hold hold the money twice | This change |
| 17 | Network reversals routed through a lookup from network identifiers to `A` | The client key is derived from identifiers the reversal echoes, so `A` is computed directly | The lookup needs the authorization's result to be visible first, which brings back the ordering race the fence removed | This change |
| 18 | `exists` treated like `ok`, with the original result rebuilt by a lookup | `ok` is now `created`, and `exists` returns the original timestamp, so no lookup is needed | Checking the facts table against TigerBeetle's current source and its 0.17.0 API changes page | This change |
| 19 | A fence's `exists` results not told apart | Only `exists_with_different_flags` means the authorization holds `A`; any other `exists` result means an earlier fence holds it | Walking a cancel's fence meeting an earlier expiry fence while explaining fencing step by step | This change |

## Open

- What to tell a client that reuses a key with a different payload (README section 12, follow-up 6).

[88e1ed4]: https://github.com/nmarasoiu/dst-200ktps-ledger/commit/88e1ed4
[f38b1ba]: https://github.com/nmarasoiu/dst-200ktps-ledger/commit/f38b1ba
[5eec5c5]: https://github.com/nmarasoiu/dst-200ktps-ledger/commit/5eec5c5
[6fb994a]: https://github.com/nmarasoiu/dst-200ktps-ledger/commit/6fb994a
[ce4e8d6]: https://github.com/nmarasoiu/dst-200ktps-ledger/commit/ce4e8d6
[6c82771]: https://github.com/nmarasoiu/dst-200ktps-ledger/commit/6c82771
[06eced4]: https://github.com/nmarasoiu/dst-200ktps-ledger/commit/06eced4
