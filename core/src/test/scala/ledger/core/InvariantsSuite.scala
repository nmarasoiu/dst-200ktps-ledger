package ledger.core

import AuthOutcome.{Approved, Declined}
import CancelOutcome.Cancelled
import CaptureOutcome.Captured
import Fixtures.*

/** A history written by hand, one record at a time. History times are milliseconds; TigerBeetle timestamps are
  * nanoseconds.
  */
final case class HistoryBuilder(
    caller: Vector[CallerRecord] = Vector.empty,
    log: Vector[LogRecord] = Vector.empty,
    edges: Vector[EdgeRecord] = Vector.empty,
    transfers: Vector[LedgerTransfer] = Vector.empty,
    accounts: Vector[LedgerAccount] = Vector.empty,
    expiries: Option[Map[TransferId, Long]] = None,
    scannedAt: Long = 1_000_000_000L,
    faultsStoppedAt: Option[Long] = None,
    endedAt: Long = 100_000L,
):
  def sent(at: Long, r: Request): HistoryBuilder = copy(caller = caller :+ CallerRecord.Sent(HistoryTime(at), r))
  def received(at: Long, r: Request, reply: Reply): HistoryBuilder =
    copy(caller = caller :+ CallerRecord.Received(HistoryTime(at), r, reply))
  def lost(at: Long, r: Request): HistoryBuilder = copy(caller = caller :+ CallerRecord.Lost(HistoryTime(at), r))
  /** A request at an edge instance, arriving at `arrived` and answered at `answered`, both by the edge's clock. */
  def edge(at: Long, r: Request, arrived: Long, answered: Option[Long], instance: Int = 1): HistoryBuilder =
    val answer = answered.map(t => (EdgeTime(t), Reply.Decided(AuthOutcome.Approved)))
    copy(edges = edges :+ EdgeRecord(instance, HistoryTime(at), r, EdgeTime(arrived), answer))
  def logged(at: Long, c: Command): HistoryBuilder = append(at, Record.CommandRecord(c))
  def decided(at: Long, c: Command, o: Outcome, by: Writer = Writer.Edge): HistoryBuilder = append(at, Record.OutcomeRecord(c, o, by))
  def ledger(t: Transfer, timestamp: Long): HistoryBuilder = copy(transfers = transfers :+ LedgerTransfer(Period(0), t, timestamp))
  def limited(id: AccountId): HistoryBuilder = copy(accounts = accounts :+ LedgerAccount(Period(0), id, debitsMustNotExceedCredits = true, creditsMustNotExceedDebits = false))

  def history: History =
    val snapshot = LedgerSnapshot(transfers, accounts, Map(Period(0) -> scannedAt), expiries)
    History(caller, log, edges, snapshot, faultsStoppedAt.map(HistoryTime(_)), HistoryTime(endedAt))

  private def append(at: Long, r: Record): HistoryBuilder =
    copy(log = log :+ LogRecord(Partition(0), Offset(log.size.toLong), HistoryTime(at), r))

object Histories:
  val k: ClientKey = key(1)
  val auth: Request.Authorize = authorize(k)
  val authC: Command.Authorize = authCommand(k).asInstanceOf[Command.Authorize]
  val a: TransferId = authC.a
  val cancel: Request.Cancel = Request.Cancel(k)
  val cancelC: Command = cancelCommand(k)
  val hold: Transfer = Transfers.authorization(authC, LedgerCfg)

  /** Authorized, logged, approved by the ledger, logged and answered. */
  val approved: HistoryBuilder = HistoryBuilder()
    .sent(0, auth)
    .logged(1, authC)
    .ledger(hold, 10)
    .decided(20, authC, Approved)
    .received(30, auth, Reply.Decided(Approved))

class InvariantsSuite extends munit.FunSuite:
  import Histories.*

  private def broken(h: HistoryBuilder): Set[String] = Invariants.check(h.history).map(_.property).toSet

  private def assertHolds(h: HistoryBuilder)(using munit.Location): Unit =
    assertEquals(Invariants.check(h.history), Vector.empty)

  test("an approved, logged and answered authorization breaks nothing"):
    assertHolds(approved)

  test(Invariants.NoTransferTwice):
    assert(broken(approved.ledger(hold, 11)).contains(Invariants.NoTransferTwice))
    val postedAndVoided = approved.ledger(Transfers.post(a, CaptureAmount.Full), 40).ledger(Transfers.void(a), 50)
    assert(broken(postedAndVoided).contains(Invariants.NoTransferTwice))

  test(Invariants.OneOutcome + ": an approval and a decline for one authorization"):
    assert(broken(approved.decided(40, authC, Declined(DeclineReason.NotExecuted), Writer.Resolver)).contains(Invariants.OneOutcome))

  test("a declined authorization, then a fenced duplicate reported not_executed, passes"):
    val rejected = Declined(DeclineReason.Ledger(TbStatus.AccountsMustBeDifferent))
    val h = HistoryBuilder()
      .sent(0, auth)
      .logged(1, authC)
      .decided(10, authC, rejected)
      .received(11, auth, Reply.Decided(rejected))
      .logged(20, cancelC)
      .ledger(Transfers.fence(a, LedgerCfg), 30)
      .decided(40, cancelC, Cancelled)
      .logged(50, authC)
      .decided(60, authC, Declined(DeclineReason.NotExecuted), Writer.Resolver)
    assertHolds(h)

  test("the resolver repeating an edge's decline with a generic reason passes"):
    val h = HistoryBuilder()
      .sent(0, auth)
      .logged(1, authC)
      .decided(10, authC, Declined(DeclineReason.Ledger(TbStatus.ExceedsCredits)))
      .received(11, auth, Reply.Decided(Declined(DeclineReason.Ledger(TbStatus.ExceedsCredits))))
      .decided(5000, authC, Declined(DeclineReason.Unknown), Writer.Resolver)
    assertHolds(h)

  test(Invariants.LoggedBeforeAnswered + ": an answer before its outcome"):
    val h = HistoryBuilder().sent(0, auth).logged(1, authC).ledger(hold, 10).received(15, auth, Reply.Decided(Approved)).decided(20, authC, Approved)
    assertEquals(broken(h), Set(Invariants.LoggedBeforeAnswered))

  test(Invariants.RefusalLeavesNoHold + ": a hidden resend creates the hold while the caller hears a refusal"):
    val h = HistoryBuilder().sent(0, auth).logged(1, authC).ledger(hold, 10).received(20, auth, Reply.Refused(RefusalReason.AcknowledgedTooLate))
    assert(broken(h).contains(Invariants.RefusalLeavesNoHold))

  test("a refused authorization resent with the same key and approved passes"):
    val h = HistoryBuilder()
      .sent(0, auth)
      .received(5, auth, Reply.Refused(RefusalReason.Overload))
      .sent(10, auth)
      .logged(11, authC)
      .ledger(hold, 20)
      .decided(30, authC, Approved)
      .received(40, auth, Reply.Decided(Approved))
    assertHolds(h)

  private val cancelledAfterApproval = approved
    .sent(100, cancel)
    .logged(101, cancelC)
    .decided(120, cancelC, Cancelled)
    .received(130, cancel, Reply.Decided(Cancelled))
    .decided(140, authC, Approved, Writer.Resolver)

  test(Invariants.CancelLeavesNoHold + ": an approval after the cancel, and the hold survives"):
    assertEquals(broken(cancelledAfterApproval), Set(Invariants.CancelLeavesNoHold))

  test("an approval after the cancel, with the hold voided, passes: the merge keeps the cancel"):
    assertHolds(cancelledAfterApproval.ledger(Transfers.void(a), 110))

  test("a hold reported cancelled that had already expired passes"):
    val expired = cancelledAfterApproval.copy(scannedAt = 10 + HoldTimeout * 1_000_000_000L)
    assertHolds(expired)

  test(Invariants.CancelLeavesNoHold + ": after a cancel is reported cancelled, a capture succeeds"):
    val capture = Request.Capture(k, CaptureAmount.Full)
    val h = cancelledAfterApproval
      .ledger(Transfers.post(a, CaptureAmount.Full), 200)
      .sent(190, capture)
      .logged(191, captureCommand(k))
      .decided(210, captureCommand(k), Captured(CaptureAmount.Full))
    assert(broken(h).contains(Invariants.CancelLeavesNoHold))
    assert(broken(h).contains(Invariants.OneOutcome))

  private val captured = approved
    .logged(100, captureCommand(k))
    .ledger(Transfers.post(a, CaptureAmount.Full), 110)
    .decided(120, captureCommand(k), Captured(CaptureAmount.Full))

  test("a hold captured once, in full, passes"):
    assertHolds(captured)

  test(Invariants.CaptureOnce + ": captured twice"):
    val again = Transfers.post(a, CaptureAmount.Full).copy(id = Ids.void(a))
    assert(broken(captured.ledger(again, 130)).contains(Invariants.CaptureOnce))

  test(Invariants.CaptureOnce + ": captured for more than it held"):
    val h = approved.ledger(Transfers.post(a, CaptureAmount.Partial(1000)), 110)
    assert(broken(h).contains(Invariants.CaptureOnce))

  test("once captured, a hold is never reported cancelled"):
    val h = captured.decided(130, cancelC, Cancelled)
    assert(broken(h).contains(Invariants.OneOutcome))
    assert(broken(h).contains(Invariants.CancelLeavesNoHold))

  test(Invariants.MatchesLedger + ": approved, but the ledger has no hold"):
    val h = HistoryBuilder().sent(0, auth).logged(1, authC).decided(20, authC, Approved).received(30, auth, Reply.Decided(Approved))
    assertEquals(broken(h), Set(Invariants.MatchesLedger))

  private val funder = AccountId.of(500)
  private def fund(amount: Long) =
    Transfer(Ids.authorization(key(99)), funder, Customer, U128.of(amount), TransferId.Zero, 1, 2, Set.empty, 0)

  test(Invariants.BalancesWithinLimits + ": a hold beyond the customer's credits"):
    val h = approved.limited(Customer)
    assertEquals(broken(h), Set(Invariants.BalancesWithinLimits))

  test("a hold within the customer's credits passes"):
    assertHolds(approved.limited(Customer).ledger(fund(100), 5))

  test("an expired hold releases its amount before the next one"):
    val second = Transfers.authorization(authCommand(key(2), amount = 100).asInstanceOf[Command.Authorize], LedgerCfg)
    val afterExpiry = HoldTimeout * 1_000_000_000L + 20
    val base = HistoryBuilder().limited(Customer).ledger(fund(150), 1).ledger(hold, 10)
    assertHolds(base.ledger(second, afterExpiry).copy(scannedAt = afterExpiry + 1))
    assert(broken(base.ledger(second, 20)).contains(Invariants.BalancesWithinLimits))

class LivenessSuite extends munit.FunSuite:
  import Histories.*
  import scala.concurrent.duration.*

  private val bounds =
    Liveness.Bounds(deadline = 1500.millis, answerWithin = 2.seconds, outcomeWithin = 10.seconds, settle = 1.second, expiryLag = 1.second)
  private def broken(h: HistoryBuilder): Set[String] = Liveness.check(h.history, bounds).map(_.property).toSet

  test("an approved, logged and answered authorization breaks nothing"):
    assertEquals(Liveness.check(approved.history, bounds), Vector.empty)

  test(Liveness.Answered + ": no answer after faults stopped"):
    val h = HistoryBuilder(faultsStoppedAt = Some(1000)).sent(5000, auth)
    assertEquals(broken(h), Set(Liveness.Answered))

  test(Liveness.Answered + ", by the edge's clock: answered after the deadline"):
    // On time by the history's clock, late by the edge's own.
    val h = HistoryBuilder().edge(at = 5000, auth, arrived = 70_000, answered = Some(71_600))
    assertEquals(broken(h), Set(Liveness.Answered))

  test(Liveness.Answered + ", by the edge's clock: never answered by a live instance"):
    assertEquals(broken(HistoryBuilder().edge(at = 5000, auth, arrived = 70_000, answered = None)), Set(Liveness.Answered))

  test("answered within the deadline by the edge's clock passes, and then the caller's side is not used"):
    val h = HistoryBuilder().edge(at = 5000, auth, arrived = 70_000, answered = Some(71_400)).sent(5000, auth)
    assertEquals(broken(h), Set.empty[String])

  test("requests sent during faults, or too close to the end, are not judged"):
    assertEquals(broken(HistoryBuilder(faultsStoppedAt = Some(10_000)).sent(5000, auth)), Set.empty[String])
    assertEquals(broken(HistoryBuilder().sent(99_500, auth)), Set.empty[String])

  test(Liveness.CommandHasOutcome):
    assertEquals(broken(HistoryBuilder().logged(1, authC)), Set(Liveness.CommandHasOutcome))
    assertEquals(broken(HistoryBuilder().logged(95_000, authC)), Set.empty[String])

  test(Liveness.CancelAcknowledged):
    val h = HistoryBuilder().sent(0, cancel).received(10, cancel, Reply.Retry)
    assertEquals(broken(h), Set(Liveness.CancelAcknowledged))
    assertEquals(broken(h.sent(20, cancel).received(30, cancel, Reply.Decided(Cancelled))), Set.empty[String])

  test(Liveness.HoldEnds):
    val due = 10 + HoldTimeout * 1_000_000_000L + 1_000_000_000L
    val held = HistoryBuilder().ledger(hold, 10).copy(scannedAt = due + 1, expiries = Some(Map.empty))
    assertEquals(broken(held), Set(Liveness.HoldEnds))
    assertEquals(broken(held.copy(expiries = Some(Map(a -> (due - 5))))), Set.empty[String])
    assertEquals(broken(HistoryBuilder().ledger(hold.copy(timeoutSeconds = 0), 10)), Set(Liveness.HoldEnds))

  test(Liveness.ThroughputFloor):
    val h = (0 until 10).foldLeft(HistoryBuilder())((b, i) => b.received(i * 100L, auth, Reply.Decided(Approved)))
    assertEquals(Liveness.throughputFloor(h.history, HistoryTime(0), HistoryTime(1000), 500.millis, 5), Vector.empty)
    assertEquals(Liveness.throughputFloor(h.history, HistoryTime(0), HistoryTime(2000), 500.millis, 5).size, 2)
