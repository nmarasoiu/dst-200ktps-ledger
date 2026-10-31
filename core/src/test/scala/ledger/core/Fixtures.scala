package ledger.core

import org.scalacheck.Gen
import scala.concurrent.duration.*

object Fixtures:
  val LedgerCfg: LedgerConfig = LedgerConfig(
    fenceDebit = AccountId.of(9001),
    fenceCredit = AccountId.of(9002),
    fenceLedger = 9,
    fenceCode = 99,
    authorizationCode = 1,
  )
  val Customer: AccountId = AccountId.of(1)
  val Merchant: AccountId = AccountId.of(2)
  val HoldTimeout: Int = 7 * 24 * 3600

  /** 2026-10-28T00:00:00Z. */
  val T0: Long = 1_793_145_600_000L

  def key(n: Int, at: Long = T0): ClientKey = ClientKey(WallTime(at), s"key-$n")
  def authorize(k: ClientKey, amount: Long = 100): Request.Authorize = Request.Authorize(k, Customer, Merchant, U128.of(amount), 1)
  def authCommand(k: ClientKey, amount: Long = 100): Command = Command.stamped(authorize(k, amount), HoldTimeout)
  def cancelCommand(k: ClientKey): Command = Command.Cancel(k)
  def captureCommand(k: ClientKey, amount: CaptureAmount = CaptureAmount.Full): Command = Command.Capture(k, amount)

  val EdgeCfg: EdgeRequest.Config = EdgeRequest.Config(
    deadline = 1500.millis,
    handOffBudget = 1200.millis,
    lateResultWait = 2.seconds,
    holdTimeoutSeconds = HoldTimeout,
    periods = PeriodSchedule.Single,
    ledger = LedgerCfg,
  )

  val ResolverCfg: ResolverPartition.Config = ResolverPartition.Config(
    deadline = 1500.millis,
    grace = 3.seconds,
    retryBackoff = 500.millis,
    periods = PeriodSchedule.Single,
    ledger = LedgerCfg,
  )

  /** What the resolver waits after a command, in log time, before judging it. */
  val Due: Long = (ResolverCfg.deadline + ResolverCfg.grace).toMillis

object Gens:
  val status: Gen[TbStatus] = Gen.oneOf(TbStatus.values.toSeq)

  val key: Gen[ClientKey] =
    for
      t <- Gen.choose(ClientKey.MinTime, ClientKey.MaxTime)
      b <- Gen.alphaNumStr
    yield ClientKey(WallTime(t), b)

  val captureAmount: Gen[CaptureAmount] =
    Gen.oneOf(Gen.const(CaptureAmount.Full), Gen.choose(1L, 1000L).map(CaptureAmount.Partial(_)))

  val declineReason: Gen[DeclineReason] = Gen.oneOf(
    Gen.const(DeclineReason.Unknown),
    Gen.const(DeclineReason.NotExecuted),
    status.map(DeclineReason.KeyReused(_)),
    status.map(DeclineReason.Ledger(_)),
  )

  val captureRefusal: Gen[CaptureRefusal] = Gen.oneOf(
    Gen.const(CaptureRefusal.NotApproved),
    Gen.const(CaptureRefusal.Cancelled),
    Gen.const(CaptureRefusal.Expired),
    Gen.const(CaptureRefusal.ExceedsHold),
    status.map(CaptureRefusal.KeyReused(_)),
    status.map(CaptureRefusal.Ledger(_)),
  )

  val outcome: Gen[Outcome] = Gen.oneOf(
    Gen.const(AuthOutcome.Approved),
    declineReason.map(AuthOutcome.Declined(_)),
    Gen.const(CancelOutcome.Cancelled),
    Gen.const(CancelOutcome.CancelRefused),
    captureAmount.map(CaptureOutcome.Captured(_)),
    captureRefusal.map(CaptureOutcome.CaptureRefused(_)),
  )
