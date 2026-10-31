package ledger.core

import scala.concurrent.duration.FiniteDuration

/** An edge instance (README section 4): every request it holds, each its own `EdgeRequest`, under admission
  * control (README section 12, gap 1). It speaks the ports. Requests arrive and are answered through the caller
  * API, and each request's writes, ledger call and wake-ups carry its `Ref`, which the reply carries back. It holds
  * nothing a restart must restore (README section 8, failover): a crash drops every request.
  */
object Edge:

  final case class Config(request: EdgeRequest.Config, admission: Admission.Config)

  /** The edge's two clocks, read at each step: the monotonic one times deadlines, and the wall one places a key's
    * period (README section 4).
    */
  final case class Now(monotonic: EdgeTime, wall: WallTime)

  /** A request on this instance: one a caller sent, named by its connection, or the edge's own cancel of one. */
  enum Ref derives CanEqual:
    case Caller(connection: Connection)
    case OwnCancel(of: Connection)

  /** A request in flight. `calledAt` is when its ledger call went out, while that call has no reply. */
  final case class InFlight(request: EdgeRequest.State, calledAt: Option[EdgeTime]) derives CanEqual

  /** Admission control counts every request in `requests`, until it ends. */
  final case class State(requests: Map[Ref, InFlight], admission: Admission.State) derives CanEqual

  object State:
    val Initial: State = State(Map.empty, Admission.State.Initial)

  enum Event derives CanEqual:
    /** Caller API: a request arrived on a new connection. */
    case Arrived(connection: Connection, request: Request)
    /** Command log: the reply to a write. The record tells a command's write from its outcome's. */
    case Written(ref: Ref, record: Record, result: CommandLog.Written)
    /** Command log: the producer's metadata changed. */
    case LeadersChanged(leaders: CommandLog.Leaders)
    /** Ledger: the reply to a request's call. */
    case Returned(ref: Ref, result: Ledger.Result)
    /** Clock: a wake-up a request asked for. */
    case Woke(ref: Ref)

  enum Effect derives CanEqual:
    case Answer(connection: Connection, reply: Reply)
    case Append(ref: Ref, append: CommandLog.Append)
    case Submit(ref: Ref, submit: Ledger.Submit)
    case WakeAt(ref: Ref, at: EdgeTime)
    case Raise(alarm: Alarm)

  def machine(cfg: Config): Machine[State, Event, Effect, Now] = Machine(State.Initial, step(_, _, _, cfg))

  def step(s: State, now: Now, e: Event, cfg: Config): Step[State, Effect] = e match
    case Event.Arrived(connection, request) => admit(s, Ref.Caller(connection), request, now, cfg)
    case Event.Written(ref, record, result) =>
      import EdgeRequest.Event.*
      val event = (record, result) match
        case (_: Record.CommandRecord, CommandLog.Written.Acknowledged) => CommandAcknowledged
        case (_: Record.CommandRecord, CommandLog.Written.Failed) => CommandFailed
        case (_: Record.OutcomeRecord, CommandLog.Written.Acknowledged) => OutcomeAcknowledged
        case (_: Record.OutcomeRecord, CommandLog.Written.Failed) => OutcomeFailed
      s.requests.get(ref).fold(replyAfterEnd(s))(advance(s, ref, _, event, now, cfg))
    case Event.Returned(ref, result) =>
      val event = result match
        case Ledger.Result.Returned(statuses) => EdgeRequest.Event.LedgerReturned(statuses)
        case Ledger.Result.Failed => EdgeRequest.Event.LedgerFailed
      s.requests.get(ref) match
        case Some(r) =>
          // The ledger's latency runs from the call to its reply.
          val sampled = r.calledAt.fold(s)(at => s.copy(admission = latency(s.admission, now.monotonic - at, cfg)))
          advance(sampled, ref, r.copy(calledAt = None), event, now, cfg)
        case None => replyAfterEnd(s)
    case Event.LeadersChanged(leaders) =>
      Step(s.copy(admission = Admission.step(s.admission, Admission.Event.LeadersChanged(leaders.leaderless), cfg.admission).state))
    case Event.Woke(ref) =>
      // Most wake-ups come after their request has ended, and change nothing.
      s.requests.get(ref).fold(Step(s))(advance(s, ref, _, EdgeRequest.Event.TimeAdvanced, now, cfg))

  /** Admission control decides first. A refusal or a retry is answered at once, with nothing written or sent. The
    * edge's own cancel passes admission control like any cancel: admitted under overload, and dropped when its
    * partition has no leader, since the caller's cancel is the guarantee (README section 8, who cancels).
    */
  private def admit(s: State, ref: Ref, request: Request, now: Now, cfg: Config): Step[State, Effect] =
    val decided = Admission.step(s.admission, Admission.Event.Arrived(request), cfg.admission)
    decided.effects.foldLeft(Step[State, Effect](s.copy(admission = decided.state), marks = decided.marks)): (acc, d) =>
      acc.flatMap: st =>
        d match
          case Admission.Decision.Admit =>
            val origin = ref match
              case Ref.Caller(_) => EdgeRequest.Origin.Caller
              case Ref.OwnCancel(_) => EdgeRequest.Origin.Edge
            run(st, ref, None, EdgeRequest.start(request, origin, now.monotonic, now.wall, cfg.request), now, cfg)
          case Admission.Decision.Refuse(reason) => Step(st, answer(ref, Reply.Refused(reason)))
          case Admission.Decision.Retry =>
            ref match
              case Ref.Caller(_) => Step(st, answer(ref, Reply.Retry))
              case Ref.OwnCancel(_) => Step(st, marks = Vector(Mark.EdgeOwnCancelDropped))

  private def advance(s: State, ref: Ref, r: InFlight, e: EdgeRequest.Event, now: Now, cfg: Config): Step[State, Effect] =
    run(s, ref, r.calledAt, EdgeRequest.step(r.request, now.monotonic, e, cfg.request), now, cfg)

  /** Places a request's new state, then carries out its effects. A request that ends leaves admission control's
    * count. If its ledger call still has no reply, the time it waited counts as a latency sample, so a ledger that
    * stops answering still raises the latency.
    */
  private def run(
      s: State,
      ref: Ref,
      calledAt: Option[EdgeTime],
      step: Step[EdgeRequest.State, EdgeRequest.Effect],
      now: Now,
      cfg: Config,
  ): Step[State, Effect] =
    val called = if step.effects.exists(_.isInstanceOf[EdgeRequest.Effect.Submit]) then Some(now.monotonic) else calledAt
    val placed =
      if step.state.phase == EdgeRequest.Phase.Done then
        val sampled = called.fold(s.admission)(at => latency(s.admission, now.monotonic - at, cfg))
        s.copy(requests = s.requests - ref, admission = Admission.step(sampled, Admission.Event.Finished, cfg.admission).state)
      else s.copy(requests = s.requests.updated(ref, InFlight(step.state, called)))
    step.effects.foldLeft(Step[State, Effect](placed, marks = step.marks))((acc, f) => acc.flatMap(carryOut(_, ref, f, now, cfg)))

  private def carryOut(s: State, ref: Ref, f: EdgeRequest.Effect, now: Now, cfg: Config): Step[State, Effect] = f match
    case EdgeRequest.Effect.AppendCommand(command) =>
      Step(s, Vector(Effect.Append(ref, CommandLog.Append(Record.CommandRecord(command)))))
    case EdgeRequest.Effect.Submit(cluster, transfers) => Step(s, Vector(Effect.Submit(ref, Ledger.Submit(cluster, transfers))))
    case EdgeRequest.Effect.AppendOutcome(command, outcome) =>
      Step(s, Vector(Effect.Append(ref, CommandLog.Append(Record.OutcomeRecord(command, outcome, Writer.Edge)))))
    case EdgeRequest.Effect.Answer(reply) => Step(s, answer(ref, reply))
    case EdgeRequest.Effect.StartCancel(key) =>
      ref match
        case Ref.Caller(connection) => admit(s, Ref.OwnCancel(connection), Request.Cancel(key), now, cfg)
        // Only an authorization a caller sent starts the edge's own cancel.
        case Ref.OwnCancel(_) => Step(s)
    case EdgeRequest.Effect.WakeAt(at) => Step(s, Vector(Effect.WakeAt(ref, at)))
    case EdgeRequest.Effect.Raise(alarm) => Step(s, Vector(Effect.Raise(alarm)))

  /** The edge's own cancel answers no one. */
  private def answer(ref: Ref, reply: Reply): Vector[Effect] = ref match
    case Ref.Caller(connection) => Vector(Effect.Answer(connection, reply))
    case Ref.OwnCancel(_) => Vector.empty

  private def latency(a: Admission.State, waited: FiniteDuration, cfg: Config): Admission.State =
    Admission.step(a, Admission.Event.LedgerLatency(waited), cfg.admission).state

  private def replyAfterEnd(s: State): Step[State, Effect] = Step(s, marks = Vector(Mark.EdgeReplyAfterEnd))
