package ledger.core

import scala.concurrent.duration.FiniteDuration

/** The edge's flow for one request (README section 4): log the command, call the ledger, log the outcome, answer,
  * all within the deadline. Each request is its own machine, so requests share no state but admission control.
  */
object EdgeRequest:

  final case class Config(
      /** The 1.5 s deadline, on the edge's monotonic clock. */
      deadline: FiniteDuration,
      /** The last moment, from arrival, to hand an authorization to the TigerBeetle client, leaving time for the
        * ledger call and the outcome write (README section 8, step budgets).
        */
      handOffBudget: FiniteDuration,
      /** How long after the deadline the edge still waits for a ledger result, to log it. */
      lateResultWait: FiniteDuration,
      /** The hold timeout stamped into each authorization: 7 days. */
      holdTimeoutSeconds: Int,
      periods: PeriodSchedule,
      ledger: LedgerConfig,
  ):
    require(handOffBudget < deadline, s"the hand-off budget $handOffBudget must leave time before the deadline $deadline")

  enum Origin:
    case Caller
    /** The edge's own cancel after an authorization timed out (README section 8, who cancels). It answers no one. */
    case Edge

  enum Phase:
    case AwaitCommandAck
    /** Handed to the TigerBeetle client: refusing is no longer possible. */
    case AwaitLedger
    case AwaitOutcomeAck(outcome: Outcome)
    /** Answered without a decision; waiting for the ledger result only to log it. */
    case AwaitLateLedger
    case Done

  final case class State(command: Command, origin: Origin, start: EdgeTime, cluster: Period, fencePath: Boolean, phase: Phase)

  enum Event:
    case CommandAcknowledged
    case CommandFailed
    case LedgerReturned(results: Vector[TbStatus])
    /** The call failed without results, so its effect is unknown. */
    case LedgerFailed
    case OutcomeAcknowledged
    case OutcomeFailed
    case TimeAdvanced

  enum Effect:
    case AppendCommand(command: Command)
    /** One call to the cluster of the key's period, through the client the process shares. */
    case Submit(cluster: Period, transfers: Vector[Transfer])
    case AppendOutcome(command: Command, outcome: Outcome)
    case Answer(reply: Reply)
    /** Start the edge's own cancel: a new request with origin `Edge`. */
    case StartCancel(key: ClientKey)
    case WakeAt(at: EdgeTime)
    case Raise(alarm: Alarm)

  import Phase.*

  /** A request admitted by admission control. `wallMillis` is the edge's wall clock, read only to place the key's
    * period (README section 4).
    */
  def start(request: Request, origin: Origin, now: EdgeTime, wallMillis: Long, cfg: Config): Step[State, Effect] =
    val command = Command.stamped(request, cfg.holdTimeoutSeconds)
    val cluster = cfg.periods.periodOf(command.key.createdAt)
    val s = State(command, origin, now, cluster, fencePath = false, AwaitCommandAck)
    val write = Vector(Effect.AppendCommand(command), Effect.WakeAt(handOffBy(s, cfg)))
    command match
      case _: Command.Authorize if !cfg.periods.hasStarted(cluster, wallMillis) =>
        // Nothing is written or sent, so the refusal is safe whatever the clocks.
        finish(s, answer(s, Reply.Refused(RefusalReason.PeriodNotStarted)), Mark.EdgeRefusedPeriodNotStarted)
      case _: Command.Authorize if cfg.periods.hasEnded(cluster, wallMillis) =>
        Step(s.copy(fencePath = true), write, Vector(Mark.EdgePeriodEndedFencePath))
      case _ => Step(s, write)

  def step(s: State, now: EdgeTime, e: Event, cfg: Config): Step[State, Effect] =
    import Event.*
    val deadline = s.start + cfg.deadline
    (s.phase, e) match
      case (AwaitCommandAck, CommandAcknowledged) if now >= handOffBy(s, cfg) =>
        refuse(s, RefusalReason.AcknowledgedTooLate, Mark.EdgeRefusedAcknowledgedTooLate)
      case (AwaitCommandAck, CommandAcknowledged) =>
        val submit = Effect.Submit(s.cluster, Transfers.forEdge(s.command, s.fencePath, cfg.ledger))
        Step(s.copy(phase = AwaitLedger), Vector(submit, Effect.WakeAt(deadline)))
      case (AwaitCommandAck, CommandFailed) =>
        refuse(s, RefusalReason.CommandNotAcknowledged, Mark.EdgeRefusedCommandNotAcknowledged)
      case (AwaitCommandAck, TimeAdvanced) if now >= handOffBy(s, cfg) =>
        refuse(s, RefusalReason.AcknowledgedTooLate, Mark.EdgeRefusedAcknowledgedTooLate)

      case (AwaitLedger, LedgerReturned(results)) =>
        Rules.decide(s.command, s.fencePath, results) match
          case Verdict.Decided(outcome, mark, alarm) if now < deadline =>
            Step(s.copy(phase = AwaitOutcomeAck(outcome)), Effect.AppendOutcome(s.command, outcome) +: raise(alarm), Vector(mark))
          case Verdict.Decided(outcome, mark, alarm) =>
            // Past the deadline: answer as on a timeout, but still log the decision (README section 4).
            timedOut(s).flatMap: done =>
              Step(done, Effect.AppendOutcome(s.command, outcome) +: raise(alarm), Vector(mark, Mark.EdgeLateDecisionLogged))
          case Verdict.Unresolved(mark, alarm) => timedOut(s).flatMap(done => Step(done, Vector(Effect.Raise(alarm)), Vector(mark)))
      case (AwaitLedger, LedgerFailed) => timedOut(s).mark(Mark.EdgeLedgerCallFailed)
      case (AwaitLedger, TimeAdvanced) if now >= deadline =>
        timedOut(s).flatMap(done => Step(done.copy(phase = AwaitLateLedger), Vector(Effect.WakeAt(deadline + cfg.lateResultWait))))

      case (AwaitOutcomeAck(outcome), OutcomeAcknowledged) if now < deadline =>
        finish(s, answer(s, Reply.Decided(outcome)), Mark.EdgeAnswered)
      case (AwaitOutcomeAck(_), OutcomeFailed) => timedOut(s).mark(Mark.EdgeOutcomeNotAcknowledged)
      case (AwaitOutcomeAck(_), OutcomeAcknowledged) => timedOut(s)
      case (AwaitOutcomeAck(_), TimeAdvanced) if now >= deadline => timedOut(s)

      case (AwaitLateLedger, LedgerReturned(results)) =>
        Rules.decide(s.command, s.fencePath, results) match
          case Verdict.Decided(outcome, mark, alarm) =>
            Step(s.copy(phase = Done), Effect.AppendOutcome(s.command, outcome) +: raise(alarm), Vector(mark, Mark.EdgeLateDecisionLogged))
          case Verdict.Unresolved(mark, alarm) => finish(s, Vector(Effect.Raise(alarm)), mark)
      case (AwaitLateLedger, LedgerFailed) => finish(s, Vector.empty, Mark.EdgeLedgerCallFailed)
      case (AwaitLateLedger, TimeAdvanced) if now >= deadline + cfg.lateResultWait =>
        finish(s, Vector.empty, Mark.EdgeLateResultAbandoned)

      // Late, duplicate or out-of-phase events change nothing.
      case _ => Step(s)

  /** An authorization must be handed to the TigerBeetle client by its budget; a cancel or a capture by the deadline. */
  def handOffBy(s: State, cfg: Config): EdgeTime =
    if s.command.kind == Kind.Authorization then s.start + cfg.handOffBudget else s.start + cfg.deadline

  /** A refusal for an authorization, which the caller may resend with the same key; retry for a cancel or a capture. */
  private def refuse(s: State, reason: RefusalReason, mark: Mark): Step[State, Effect] = s.command match
    case _: Command.Authorize => finish(s, answer(s, Reply.Refused(reason)), mark)
    case _ => finish(s, answer(s, Reply.Retry), mark)

  /** No definitive answer: an authorization times out and the edge cancels it; a cancel or a capture is retried. */
  private def timedOut(s: State): Step[State, Effect] = s.command match
    case c: Command.Authorize =>
      val cancel = if s.origin == Origin.Caller then Vector(Effect.StartCancel(c.key)) else Vector.empty
      finish(s, answer(s, Reply.TimedOut) ++ cancel, Mark.EdgeTimedOutAuthorization)
    case _ => finish(s, answer(s, Reply.Retry), Mark.EdgeTimedOutRetry)

  private def finish(s: State, effects: Vector[Effect], marks: Mark*): Step[State, Effect] =
    Step(s.copy(phase = Done), effects, marks.toVector)

  private def answer(s: State, reply: Reply): Vector[Effect] =
    if s.origin == Origin.Caller then Vector(Effect.Answer(reply)) else Vector.empty

  private def raise(alarm: Option[Alarm]): Vector[Effect] = alarm.map(Effect.Raise(_)).toVector
