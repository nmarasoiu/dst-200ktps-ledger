package ledger.sim

import ledger.core.*

/** Deliberate bugs the simulator must catch, since a simulator that never fails proves nothing (README section 14).
  * A bug planted in a process rewrites that process's effects, so the core carries no switches for bugs (README
  * section 11, processes). `caughtBy` is the property that must break.
  */
enum Planted(val caughtBy: String) derives CanEqual:
  /** Records an authorization expired from the clock alone, so a hold the edge did create contradicts it. */
  case ResolverExpiresByTheClock extends Planted(Invariants.MatchesLedger)
  /** Submits a straggling authorization instead of fencing it, so a refused copy can still create a hold. */
  case ResolverSubmitsInsteadOfFencing extends Planted(Invariants.RefusalLeavesNoHold)
  /** Voids without taking `A`, so a late authorization still creates its hold after `cancelled`. */
  case CancelVoidsWithoutFencing extends Planted(Invariants.CancelLeavesNoHold)
  /** Calls TigerBeetle when it starts the write, so a write that fails leaves a hold nothing logged. */
  case EdgeCallsLedgerBeforeAck extends Planted(Invariants.WritesFollowCommands)
  /** Answers when it starts the outcome's write, so a caller can hear a decision the log does not have yet. */
  case EdgeAnswersBeforeOutcomeAck extends Planted(Invariants.LoggedBeforeAnswered)
  /** Commits to the end of the partition, so a restart skips a command with no outcome. */
  case ResolverCommitsPastOutcomeless extends Planted(Liveness.CommandHasOutcome)
  /** A hop between the caller and the edge resends on its own after losing an answer (README section 5). */
  case HopResendsAnAuthorization extends Planted(Invariants.RefusalLeavesNoHold)
  /** Voids a hold when a capture times out, so a hold is released with no cancel asked for. */
  case EdgeCancelsWhenCaptureTimesOut extends Planted(Invariants.WritesFollowCommands)
  /** Sends a full capture as amount 0, which posts nothing and releases the hold (README section 8, captures). */
  case CaptureSentAsZero extends Planted(Invariants.MatchesLedger)
  /** Fences a capture it finds without an outcome, though a capture is never fenced. */
  case ResolverFencesCapture extends Planted(Pipeline.AlarmProperty)
  /** Drops a cancel it should resubmit, so the command never gets an outcome. */
  case ResolverDropsCancel extends Planted(Liveness.CommandHasOutcome)
  /** Goes on as if it wrote each outcome, so the commands stay without one. */
  case ResolverDropsOutcomes extends Planted(Liveness.CommandHasOutcome)
  /** Sends one cancel and then gives up, whatever it was told. */
  case CallerStopsCancelling extends Planted(Liveness.CancelAcknowledged)
  /** Sits on every answer for two seconds. */
  case EdgeMissesItsDeadline extends Planted(Liveness.Answered)

object Planted:
  /** A copy whose answer never reaches the caller. */
  val AnswerLost: Long = -1L

  def isFence(ref: Resolver.Ref, submit: Ledger.Submit): Boolean =
    ref.id.kind == Kind.Authorization && submit.transfers.size == 1 && submit.transfers.head.flags.isEmpty

  /** What a fence would have said, given what its void found. */
  def pretendFenced(statuses: Vector[TbStatus]): Vector[TbStatus] = statuses match
    case Vector(void) =>
      val fence = void.group match
        case TbGroup.PendingNotPending => TbStatus.Created
        case TbGroup.Transient(_) | TbGroup.IdAlreadyFailed => TbStatus.IdAlreadyFailed
        case _ => TbStatus.ExistsWithDifferentFlags
      Vector(fence, void)
    case other => other

  def onEdge(bug: Planted, f: Edge.Effect, cfg: LedgerConfig): Vector[Edge.Effect] = (bug, f) match
    case (EdgeCallsLedgerBeforeAck, Edge.Effect.Append(ref, CommandLog.Append(Record.CommandRecord(c: Command.Authorize)))) =>
      Vector(f, Edge.Effect.Submit(ref, Ledger.Submit(Period(0), Vector(Transfers.authorization(c, cfg)))))
    case (EdgeAnswersBeforeOutcomeAck, Edge.Effect.Append(Edge.Ref.Caller(connection), CommandLog.Append(Record.OutcomeRecord(_, outcome, _)))) =>
      Vector(Edge.Effect.Answer(connection, Reply.Decided(outcome)), f)
    case (CaptureSentAsZero, Edge.Effect.Submit(ref, submit)) =>
      Vector(Edge.Effect.Submit(ref, submit.copy(transfers = submit.transfers.map(zeroIfFull))))
    case (EdgeCancelsWhenCaptureTimesOut, Edge.Effect.Submit(ref, submit)) if submit.transfers.exists(_.isPost) =>
      Vector(f, Edge.Effect.Submit(ref, submit.copy(transfers = Vector(Transfers.void(submit.transfers.head.pendingId)))))
    case _ => Vector(f)

  private def zeroIfFull(t: Transfer): Transfer = if t.isPost && t.amount.isMax then t.copy(amount = U128.Zero) else t

  def onResolver(bug: Planted, f: Resolver.Effect, s: Resolver.State, kafka: SimKafka, cfg: LedgerConfig): Vector[Resolver.Effect] = (bug, f) match
    case (ResolverSubmitsInsteadOfFencing, Resolver.Effect.Submit(ref, submit)) if isFence(ref, submit) =>
      s.partitions.get(ref.partition).flatMap(_.waiting.get(ref.id)).map(_.command) match
        case Some(c: Command.Authorize) => Vector(Resolver.Effect.Submit(ref, submit.copy(transfers = Vector(Transfers.authorization(c, cfg)))))
        case _ => Vector(f)
    case (ResolverCommitsPastOutcomeless, Resolver.Effect.Commit(c)) =>
      Vector(Resolver.Effect.Commit(c.copy(next = kafka.endOffset(c.partition))))
    case (ResolverFencesCapture, Resolver.Effect.Submit(ref, submit)) if ref.id.kind == Kind.Capture =>
      Vector(Resolver.Effect.Submit(ref, submit.copy(transfers = Vector(Transfers.fence(ref.id.a, cfg)))))
    case (ResolverDropsCancel, Resolver.Effect.Submit(ref, _)) if ref.id.kind == Kind.Cancel => Vector.empty
    case _ => Vector(f)
