package ledger.core

import AuthOutcome.{Approved, Declined}
import DeclineReason.{NotExecuted, Unknown}
import TbGroup as G

/** What a rule makes of TigerBeetle's results. */
enum Verdict[+O]:
  case Decided(outcome: O, mark: Mark, alarm: Option[Alarm])
  /** No definitive outcome: nothing safe can be said. The edge answers as on a timeout, and the resolver leaves
    * the command without an outcome and tries again later.
    */
  case Unresolved(mark: Mark, alarm: Alarm)

/** The rules: each maps TigerBeetle's results for one kind of command to an outcome, one case per table row
  * (README sections 5 and 8). The edge and the resolver use the same rules (README section 2, principle 5).
  */
object Rules:

  /** An authorization the edge submitted as a pending transfer with ID `A`. */
  def authorization(status: TbStatus): Verdict[AuthOutcome] = status.group match
    case G.Created => decided(Approved, Mark.AuthorizationCreated)
    case G.Exists => decided(Approved, Mark.AuthorizationExists)
    case G.ExistsWithDifferentFlags => decided(Declined(NotExecuted), Mark.AuthorizationFenced)
    case G.ExistsWithDifferentField(s) =>
      decided(Declined(DeclineReason.KeyReused(s)), Mark.AuthorizationKeyReused, Alarm.KeyReused(s))
    case G.IdAlreadyFailed => decided(Declined(Unknown), Mark.AuthorizationIdAlreadyFailed)
    case G.Transient(s) => decided(Declined(DeclineReason.Ledger(s)), Mark.AuthorizationDeclined)
    case G.Rejected(s) => decided(Declined(DeclineReason.Ledger(s)), Mark.AuthorizationRejected)
    case G.PendingNotPending | G.AlreadyPosted | G.AlreadyVoided | G.PendingExpired | G.ExceedsPendingAmount =>
      // Codes of a post or a void, which a pending transfer never gets. The hold was not created, so the decline
      // is true; the alarm flags the impossible result.
      decided(Declined(DeclineReason.Ledger(status)), Mark.AuthorizationRejected, unexpected(Kind.Authorization, status))

  /** The fence for `A`, written instead of an authorization: by the resolver for a straggler, or by the edge when
    * the authorization's period has ended. The ledger decides, not a clock (README section 8, resolver).
    */
  def expiry(fence: TbStatus): Verdict[AuthOutcome] = fence.group match
    case G.ExistsWithDifferentFlags => decided(Approved, Mark.ExpiryFoundAuthorization)
    case G.IdAlreadyFailed => decided(Declined(Unknown), Mark.ExpiryFoundDeclined)
    case G.Created => decided(Declined(NotExecuted), Mark.ExpiryFenced)
    case G.Exists => decided(Declined(NotExecuted), Mark.ExpiryFoundFence)
    case G.Transient(s) => decided(Declined(NotExecuted), Mark.ExpiryFenceBurned, Alarm.FenceBurned(s))
    case G.ExistsWithDifferentField(s) => decided(Declined(NotExecuted), Mark.ExpiryFenceChanged, Alarm.FenceChanged(s))
    case G.Rejected(_) | G.PendingNotPending | G.AlreadyPosted | G.AlreadyVoided | G.PendingExpired |
        G.ExceedsPendingAmount =>
      // A rejected fence was not recorded, so `A` is not fenced and a late authorization could still create it.
      Verdict.Unresolved(Mark.ExpiryUnresolved, Alarm.UnexpectedResults(Kind.Authorization, Vector(fence)))

  /** A cancel: the fence `A` and the void `V`, in one batch, fence first (README section 8, the fence table).
    * Only rows that guarantee no hold survives report `cancelled`.
    */
  def cancel(fence: TbStatus, void: TbStatus): Verdict[CancelOutcome] = (fence.group, void.group) match
    case (G.ExistsWithDifferentFlags, G.Created) => decided(CancelOutcome.Cancelled, Mark.CancelReleasedHold)
    case (G.ExistsWithDifferentFlags, G.Exists) => decided(CancelOutcome.Cancelled, Mark.CancelReplayedVoid)
    case (G.ExistsWithDifferentFlags, G.PendingExpired) => decided(CancelOutcome.Cancelled, Mark.CancelFoundHoldExpired)
    case (G.ExistsWithDifferentFlags, G.AlreadyPosted) => decided(CancelOutcome.CancelRefused, Mark.CancelFoundCapture)
    case (G.IdAlreadyFailed, G.Transient(TbStatus.PendingTransferNotFound)) =>
      decided(CancelOutcome.Cancelled, Mark.CancelNothingHeld)
    case (G.IdAlreadyFailed, G.IdAlreadyFailed) => decided(CancelOutcome.Cancelled, Mark.CancelReplayedNothingHeld)
    case (G.Created, G.PendingNotPending) => decided(CancelOutcome.Cancelled, Mark.CancelFencedFirst)
    case (G.Exists, G.PendingNotPending) => decided(CancelOutcome.Cancelled, Mark.CancelFoundFence)
    case (G.Transient(s), G.Transient(TbStatus.PendingTransferNotFound) | G.IdAlreadyFailed) =>
      // The ID check runs first, so a transient failure means `A` did not exist; it is burned now.
      decided(CancelOutcome.Cancelled, Mark.CancelFenceBurned, Alarm.FenceBurned(s))
    case (G.ExistsWithDifferentField(s), G.PendingNotPending) =>
      decided(CancelOutcome.Cancelled, Mark.CancelFenceChanged, Alarm.FenceChanged(s))
    case _ => Verdict.Unresolved(Mark.CancelUnresolved, Alarm.UnexpectedResults(Kind.Cancel, Vector(fence, void)))

  /** A capture: the post `P` of the hold `A` (README section 8, the capture table). Never fenced or expired. */
  def capture(post: TbStatus, amount: CaptureAmount): Verdict[CaptureOutcome] =
    import CaptureOutcome.{CaptureRefused, Captured}
    post.group match
      case G.Created => decided(Captured(amount), Mark.CaptureCreated)
      case G.Exists => decided(Captured(amount), Mark.CaptureReplayed)
      case G.ExistsWithDifferentField(s) =>
        decided(CaptureRefused(CaptureRefusal.KeyReused(s)), Mark.CaptureKeyReused, Alarm.KeyReused(s))
      case G.AlreadyVoided => decided(CaptureRefused(CaptureRefusal.Cancelled), Mark.CaptureFoundVoid)
      case G.PendingExpired => decided(CaptureRefused(CaptureRefusal.Expired), Mark.CaptureFoundExpired)
      case G.ExceedsPendingAmount => decided(CaptureRefused(CaptureRefusal.ExceedsHold), Mark.CaptureExceedsHold)
      case G.PendingNotPending => decided(CaptureRefused(CaptureRefusal.NotApproved), Mark.CaptureFoundFence)
      case G.Transient(TbStatus.PendingTransferNotFound) =>
        decided(CaptureRefused(CaptureRefusal.NotApproved), Mark.CaptureFoundNoHold)
      case G.IdAlreadyFailed => decided(CaptureRefused(CaptureRefusal.NotApproved), Mark.CaptureReplayedNoHold)
      case G.Transient(s) => decided(CaptureRefused(CaptureRefusal.Ledger(s)), Mark.CaptureRejected)
      case G.Rejected(s) => decided(CaptureRefused(CaptureRefusal.Ledger(s)), Mark.CaptureRejected)
      case G.ExistsWithDifferentFlags | G.AlreadyPosted =>
        // `P` is only ever a post, and only `P` posts the hold, so neither can happen.
        Verdict.Unresolved(Mark.CaptureUnresolved, Alarm.UnexpectedResults(Kind.Capture, Vector(post)))

  /** The results of one ledger call for a command. `fencePath` is true when the authorization was fenced rather
    * than submitted: always for the resolver, and for the edge once the period has ended.
    */
  def decide(command: Command, fencePath: Boolean, results: Vector[TbStatus]): Verdict[Outcome] =
    (command, results) match
      case (_: Command.Authorize, Vector(r)) => if fencePath then expiry(r) else authorization(r)
      case (_: Command.Cancel, Vector(fence, void)) => cancel(fence, void)
      case (c: Command.Capture, Vector(post)) => capture(post, c.amount)
      case (c, _) =>
        val mark = c.kind match
          case Kind.Authorization => if fencePath then Mark.ExpiryUnresolved else Mark.AuthorizationUnresolved
          case Kind.Cancel => Mark.CancelUnresolved
          case Kind.Capture => Mark.CaptureUnresolved
        Verdict.Unresolved(mark, Alarm.UnexpectedResults(c.kind, results))

  private def decided[O](outcome: O, mark: Mark): Verdict[O] = Verdict.Decided(outcome, mark, None)

  private def decided[O](outcome: O, mark: Mark, alarm: Alarm): Verdict[O] = Verdict.Decided(outcome, mark, Some(alarm))

  private def unexpected(kind: Kind, status: TbStatus): Alarm = Alarm.UnexpectedResults(kind, Vector(status))
