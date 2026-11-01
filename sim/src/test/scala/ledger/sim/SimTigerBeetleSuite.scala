package ledger.sim

import ledger.core.*
import ledger.core.TbStatus.*

/** What the contract suite cannot pin on a real server, whose pulses it does not control. The rest of this adapter's
  * behavior is held to the real system by that suite.
  */
class SimTigerBeetleSuite extends munit.FunSuite:
  private val Second = 1_000_000_000L
  private val (debit, credit) = (AccountId.of(1), AccountId.of(2))
  private val cfg = LedgerConfig(AccountId.of(8), AccountId.of(9), 99, 90, 71)

  private def ledger(): SimTigerBeetle =
    val tb = new SimTigerBeetle(Period(0))
    tb.createAccounts(
      0L,
      Vector(
        Ledger.NewAccount(debit, 7, 70),
        Ledger.NewAccount(credit, 7, 70),
        Ledger.NewAccount(cfg.fenceDebit, 99, 90),
        Ledger.NewAccount(cfg.fenceCredit, 99, 90),
      ),
    )
    tb

  private def key(n: Int) = ClientKey(WallTime(1000L + n), s"k$n")

  private def hold(n: Int, timeout: Int = 1, amount: Long = 100): Transfer =
    Transfers.authorization(Command.Authorize(key(n), debit, credit, U128.of(amount), 7, timeout), cfg)

  private def pending(tb: SimTigerBeetle): Long = tb.lookupAccounts(Vector(debit)).head.debitsPending.lo

  test("a hold is refused from its timeout on, before any pulse has released its money"):
    val tb = ledger()
    assertEquals(tb.createTransfers(10L, Vector(hold(1))), Vector(Created))
    val heldAt = tb.scan(0L, 1).head.timestamp
    val a = Ids.authorization(key(1))
    assertEquals(tb.createTransfers(heldAt + Second, Vector(Transfers.post(a, CaptureAmount.Full))), Vector(PendingTransferExpired))
    assertEquals(tb.createTransfers(heldAt + Second, Vector(Transfers.void(a))), Vector(PendingTransferExpired))
    assertEquals(pending(tb), 100L)
    tb.pulse(heldAt + Second)
    assertEquals(pending(tb), 0L)

  test("a hold can still be closed one nanosecond before its timeout, and then no pulse expires it"):
    val tb = ledger()
    assertEquals(tb.createTransfers(10L, Vector(hold(1))), Vector(Created))
    val heldAt = tb.scan(0L, 1).head.timestamp
    val a = Ids.authorization(key(1))
    assertEquals(tb.createTransfers(heldAt + Second - 2, Vector(Transfers.void(a))), Vector(Created))
    tb.pulse(heldAt + 2 * Second)
    assertEquals(tb.snapshot.expiries, Some(Map.empty[TransferId, Long]))

  test("a hold with no timeout never expires"):
    val tb = ledger()
    assertEquals(tb.createTransfers(10L, Vector(hold(1, timeout = 0))), Vector(Created))
    tb.pulse(1000 * Second)
    assertEquals(pending(tb), 100L)

  test("a pulse expires only what is due, up to one batch, oldest first, and records when"):
    val tb = ledger()
    val count = SimTigerBeetle.ExpiryBatch + 5
    val holds = (1 to count).toVector.map(hold(_))
    assertEquals(tb.createTransfers(10L, holds).distinct, Vector(Created))
    val after = tb.scan(0L, 1).head.timestamp + 2 * Second
    tb.pulse(1L)
    assertEquals(pending(tb), 100L * count)
    tb.pulse(after)
    assertEquals(pending(tb), 500L)
    val first = tb.snapshot.expiries.get
    assertEquals(first.size, SimTigerBeetle.ExpiryBatch)
    assert(first.contains(holds.head.id))
    assert(!first.contains(holds.last.id))
    tb.pulse(after + 1)
    assertEquals(pending(tb), 0L)
    val all = tb.snapshot.expiries.get
    assertEquals(all.size, count)
    assertEquals(all.values.toSet.size, count)

  test("a timestamp never goes back, even when the given clock does"):
    val tb = ledger()
    assertEquals(tb.createTransfers(1000L, Vector(hold(1), hold(2))), Vector(Created, Created))
    assertEquals(tb.createTransfers(5L, Vector(hold(3))), Vector(Created))
    tb.pulse(1L)
    assertEquals(tb.createTransfers(6L, Vector(hold(4))), Vector(Created))
    val times = tb.scan(0L, 10).map(_.timestamp)
    assertEquals(times, times.sorted.distinct)
    assertEquals(times.size, 4)

  test("a batch's events take consecutive timestamps, the last one no earlier than the clock"):
    val tb = ledger()
    assertEquals(tb.createTransfers(1000L, Vector(hold(1), hold(2), hold(3))).size, 3)
    assertEquals(tb.scan(0L, 10).map(_.timestamp), Vector(998L, 999L, 1000L))

  test("the snapshot is what the invariants read, and they hold on it"):
    val tb = ledger()
    val a = Ids.authorization(key(1))
    assertEquals(tb.createTransfers(1000L, Vector(hold(1), hold(2), hold(3))).distinct, Vector(Created))
    assertEquals(tb.createTransfers(2000L, Vector(Transfers.post(a, CaptureAmount.Partial(40)))), Vector(Created))
    assertEquals(tb.createTransfers(3000L, Vector(Transfers.void(Ids.authorization(key(2))))), Vector(Created))
    tb.pulse(10 * Second)
    val snapshot = tb.snapshot
    assertEquals(snapshot.accounts.map(_.id), Vector(debit, credit, cfg.fenceDebit, cfg.fenceCredit))
    assertEquals(snapshot.expiries.map(_.keySet), Some(Set(Ids.authorization(key(3)))))
    assertEquals(snapshot.postedAmount(snapshot.posts(a).head, snapshot.pending(a).get), U128.of(40))
    assertEquals(snapshot.scannedAt.keySet, Set(Period(0)))
    val history = History(Vector.empty, Vector.empty, Vector.empty, snapshot, None, HistoryTime(0))
    assertEquals(Invariants.check(history), Vector.empty)
