package ledger.core

/** A TigerBeetle account ID. */
opaque type AccountId = U128

object AccountId:
  val Zero: AccountId = U128.Zero
  def apply(value: U128): AccountId = value
  def of(n: Long): AccountId = U128.of(n)
  extension (id: AccountId) def value: U128 = id

enum Kind:
  case Authorization, Cancel, Capture

/** One command's identity. Copies with the same key and kind are copies of one command: an outcome for one
  * answers them all.
  */
final case class CommandId(a: TransferId, kind: Kind)

/** A capture amount. Full and partial are separate cases and a partial amount is positive, so a raw 0 never
  * reaches TigerBeetle as a capture (README section 8, captures).
  */
sealed trait CaptureAmount

object CaptureAmount:
  case object Full extends CaptureAmount

  final case class Partial private (minorUnits: Long) extends CaptureAmount

  object Partial:
    def from(minorUnits: Long): Either[String, Partial] =
      if minorUnits > 0 then Right(new Partial(minorUnits)) else Left(s"a partial capture must be positive: $minorUnits")

    def apply(minorUnits: Long): Partial = from(minorUnits).fold(e => throw IllegalArgumentException(e), identity)

/** What a caller sends (README section 4). */
enum Request:
  case Authorize(key: ClientKey, debit: AccountId, credit: AccountId, amount: U128, ledger: Int)
  case Cancel(key: ClientKey)
  case Capture(key: ClientKey, amount: CaptureAmount)

  def key: ClientKey

  def kind: Kind = this match
    case _: Authorize => Kind.Authorization
    case _: Cancel => Kind.Cancel
    case _: Capture => Kind.Capture

  def a: TransferId = Ids.authorization(key)
  def id: CommandId = CommandId(a, kind)

/** What the edge logs: the request, with the hold timeout the edge stamped into an authorization. Every copy of
  * the command carries the stamped value, so it is part of a retry's identity (README section 8, captures).
  * Commands carry no deadline: the resolver measures it from the append time (README section 8, deadlines).
  */
enum Command:
  case Authorize(key: ClientKey, debit: AccountId, credit: AccountId, amount: U128, ledger: Int, holdTimeoutSeconds: Int)
  case Cancel(key: ClientKey)
  case Capture(key: ClientKey, amount: CaptureAmount)

  def key: ClientKey

  def kind: Kind = this match
    case _: Authorize => Kind.Authorization
    case _: Cancel => Kind.Cancel
    case _: Capture => Kind.Capture

  /** The authorization's ID, which is also the Kafka record key of every command about the hold. */
  lazy val a: TransferId = Ids.authorization(key)
  def id: CommandId = CommandId(a, kind)

object Command:
  def stamped(request: Request, holdTimeoutSeconds: Int): Command = request match
    case Request.Authorize(key, debit, credit, amount, ledger) => Authorize(key, debit, credit, amount, ledger, holdTimeoutSeconds)
    case Request.Cancel(key) => Cancel(key)
    case Request.Capture(key, amount) => Capture(key, amount)

/** How a command ended. An authorization ends in a decision; the reason for a decline is best-effort detail
  * (README section 5, outcomes are decisions).
  */
sealed trait Outcome:
  def decision: Decision = this match
    case AuthOutcome.Approved => Decision.Approved
    case AuthOutcome.Declined(_) => Decision.Declined
    case CancelOutcome.Cancelled => Decision.Cancelled
    case CancelOutcome.CancelRefused => Decision.CancelRefused
    case CaptureOutcome.Captured(_) => Decision.Captured
    case CaptureOutcome.CaptureRefused(_) => Decision.CaptureRefused

enum AuthOutcome extends Outcome:
  case Approved
  case Declined(reason: DeclineReason)

enum CancelOutcome extends Outcome:
  case Cancelled
  /** The hold was already captured. */
  case CancelRefused

enum CaptureOutcome extends Outcome:
  case Captured(amount: CaptureAmount)
  case CaptureRefused(reason: CaptureRefusal)

/** An outcome without its detail: what invariants compare. */
enum Decision:
  case Approved, Declined, Cancelled, CancelRefused, Captured, CaptureRefused

enum DeclineReason:
  /** Declined earlier, reason unknown: the fence found `id_already_failed`. */
  case Unknown
  /** `A` is fenced, whether by expiry or by a cancel. */
  case NotExecuted
  /** The key was reused with a different payload (README section 12, follow-up 6; still open). */
  case KeyReused(status: TbStatus)
  /** TigerBeetle's own reason: a transient failure, or a validation failure it did not record. */
  case Ledger(status: TbStatus)

object DeclineReason:
  /** From generic to specific. The merge keeps the most specific reason (README section 5, one merge). */
  given Ordering[DeclineReason] = Ordering.by: r =>
    r match
      case KeyReused(s) => (r.ordinal, s.ordinal)
      case Ledger(s) => (r.ordinal, s.ordinal)
      case _ => (r.ordinal, -1)

enum CaptureRefusal:
  /** No hold to post: a fence, no hold at all, or a resend after either. */
  case NotApproved
  /** The hold was voided by a cancel. */
  case Cancelled
  /** The hold was past its timeout. */
  case Expired
  /** The amount exceeds the hold. Not recorded, so a corrected capture may reuse `P`. */
  case ExceedsHold
  /** `P` exists with a different amount (README section 12, follow-up 6; still open). */
  case KeyReused(status: TbStatus)
  /** Another rejection by TigerBeetle. */
  case Ledger(status: TbStatus)

object CaptureRefusal:
  given Ordering[CaptureRefusal] = Ordering.by: r =>
    r match
      case KeyReused(s) => (r.ordinal, s.ordinal)
      case Ledger(s) => (r.ordinal, s.ordinal)
      case _ => (r.ordinal, -1)

/** Why the edge refused a request before handing it to the TigerBeetle client. A refusal is definitive for the
  * copy it refuses (README section 5, refusals).
  */
enum RefusalReason:
  case Overload
  case NoLeader
  case CommandNotAcknowledged
  case AcknowledgedTooLate
  case PeriodNotStarted

/** What the edge answers. */
enum Reply:
  case Decided(outcome: Outcome)
  /** An authorization the edge never sent: the caller may resend it with the same key. */
  case Refused(reason: RefusalReason)
  /** An authorization without a definitive answer: the caller must cancel. */
  case TimedOut
  /** A cancel or a capture without an answer: the caller resends it with the same key. */
  case Retry

enum Writer:
  case Edge, Resolver

/** A record of the command log, keyed by `A` (README section 4). */
enum Record:
  case CommandRecord(command: Command)
  case OutcomeRecord(command: Command, outcome: Outcome, writer: Writer)

  def command: Command

/** Something an operator must look at. None of them stops the pipeline. */
enum Alarm:
  /** A fence failed with a transient code: the fence accounts are missing or closed. `A` is burned anyway. */
  case FenceBurned(status: TbStatus)
  /** A fence met an earlier fence with different fields: the fence's fields changed (README section 5). */
  case FenceChanged(status: TbStatus)
  /** A client key reused with a different payload: a caller bug (README section 12, follow-up 6). */
  case KeyReused(status: TbStatus)
  /** Results no table row lists. No definitive outcome is given for them. */
  case UnexpectedResults(kind: Kind, statuses: Vector[TbStatus])
