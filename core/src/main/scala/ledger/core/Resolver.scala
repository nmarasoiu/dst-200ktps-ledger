package ledger.core

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.FiniteDuration

/** One assignment of a partition to the resolver. Results tagged with an older one, from before a rebalance, are
  * dropped. An opaque `Long`: stored as a primitive, and never mistaken for an offset or a time.
  */
opaque type Generation = Long

object Generation:
  val Initial: Generation = 0L
  def apply(n: Long): Generation = n
  extension (g: Generation) def next: Generation = g + 1
  given CanEqual[Generation, Generation] = CanEqual.derived

/** The resolver on one assigned partition of the command log (README section 8, resolver). It gives an outcome to
  * every command still without one a grace period after its deadline, judged in log time; it fences an
  * authorization and resubmits a cancel or a capture. It never submits an authorization, and it commits offsets
  * only up to its oldest command still without an outcome.
  */
object ResolverPartition:

  final case class Config(
      deadline: FiniteDuration,
      grace: FiniteDuration,
      /** How long to wait before trying again after a failed call, a failed write or an unresolved result. */
      retryBackoff: FiniteDuration,
      periods: PeriodSchedule,
      ledger: LedgerConfig,
  )

  enum Phase derives CanEqual:
    case Idle(retryAt: Option[ResolverTime])
    case Submitting
    case Appending(outcome: Outcome)

  /** A command read with no outcome after it yet. `first` and `appended` are those of its earliest copy. */
  final case class Waiting(first: Offset, appended: LogTime, command: Command, phase: Phase) derives CanEqual

  /** `logNow` is the latest append time read: the partition's own clock. `endSeenAt` is when a poll last reached
    * the end of the partition, by the resolver's clock; a new record clears it.
    */
  final case class State(
      partition: Partition,
      generation: Generation,
      waiting: Map[CommandId, Waiting],
      order: SortedMap[Offset, CommandId],
      next: Offset,
      logNow: Option[LogTime],
      endSeenAt: Option[ResolverTime],
      committed: Offset,
  ) derives CanEqual

  /** A partition just assigned, read again from its committed offset. Results tagged with an older generation,
    * from before a rebalance, are dropped.
    */
  def assigned(partition: Partition, generation: Generation, committed: Offset): State =
    State(partition, generation, Map.empty, SortedMap.empty, committed, None, None, committed)

  enum Event derives CanEqual:
    case Read(offset: Offset, appended: LogTime, record: Record)
    /** A poll found nothing past `next`: the end of the partition (README section 6, end offsets). */
    case ReachedEnd
    case LedgerReturned(id: CommandId, generation: Generation, results: Vector[TbStatus])
    case LedgerFailed(id: CommandId, generation: Generation)
    case OutcomeAcknowledged(id: CommandId, generation: Generation)
    case OutcomeFailed(id: CommandId, generation: Generation)
    case TimeAdvanced

  enum Effect derives CanEqual:
    case Submit(id: CommandId, generation: Generation, cluster: Period, transfers: Vector[Transfer])
    case AppendOutcome(id: CommandId, generation: Generation, command: Command, outcome: Outcome)
    case Commit(partition: Partition, next: Offset)
    case Raise(alarm: Alarm)
    case WakeAt(at: ResolverTime)

  def step(s: State, now: ResolverTime, e: Event, cfg: Config): Step[State, Effect] =
    applyEvent(s, now, e, cfg).flatMap(act(_, now, cfg)).flatMap(commit)

  /** A command is a straggler once a record appended the deadline plus the grace period after it has been read on
    * its partition, or the end of an idle partition has been reached that long after it. Never by the clock alone
    * while records remain to be read, so a resolver that restarts or lags reads past an outcome before judging.
    */
  def isStraggler(w: Waiting, s: State, cfg: Config): Boolean =
    val due = w.appended + cfg.deadline + cfg.grace
    s.logNow.exists(_ >= due) || s.endSeenAt.exists(_ >= due.onResolverClock)

  private def applyEvent(s: State, now: ResolverTime, e: Event, cfg: Config): Step[State, Effect] = e match
    case Event.Read(offset, _, _) if offset.value < s.next.value => Step(s, marks = Vector(Mark.ResolverRedelivered))
    case Event.Read(offset, appended, record) =>
      val logNow = s.logNow.fold(appended)(t => if appended > t then appended else t)
      val read = s.copy(next = offset.next, logNow = Some(logNow), endSeenAt = None)
      record match
        case Record.CommandRecord(command) if read.waiting.contains(command.id) => Step(read)
        case Record.CommandRecord(command) =>
          val w = Waiting(offset, appended, command, Phase.Idle(None))
          Step(read.copy(waiting = read.waiting.updated(command.id, w), order = read.order.updated(offset, command.id)))
        case Record.OutcomeRecord(command, _, _) =>
          read.waiting.get(command.id) match
            case None => Step(read)
            case Some(w) =>
              val inFlight = w.phase match
                case Phase.Idle(_) => Vector.empty
                case _ => Vector(Mark.ResolverOutcomeWhileInFlight)
              Step(remove(read, command.id), marks = Mark.ResolverFoundOutcome +: inFlight)
    case Event.ReachedEnd => Step(s.copy(endSeenAt = Some(now)))
    case Event.LedgerReturned(id, generation, results) =>
      current(s, id, generation) match
        case Some(w @ Waiting(_, _, command, Phase.Submitting)) =>
          Rules.decide(command, fencePath = true, results) match
            case Verdict.Decided(outcome, mark, alarm) =>
              val append = Effect.AppendOutcome(id, generation, command, outcome)
              Step(update(s, w.copy(phase = Phase.Appending(outcome))), append +: alarm.map(Effect.Raise(_)).toVector, Vector(mark))
            case Verdict.Unresolved(mark, alarm) =>
              retryLater(s, w, now, cfg).flatMap(r => Step(r, Vector(Effect.Raise(alarm)), Vector(mark)))
        case _ => Step(s, marks = Vector(Mark.ResolverStaleResult))
    case Event.LedgerFailed(id, generation) =>
      current(s, id, generation) match
        case Some(w @ Waiting(_, _, _, Phase.Submitting)) => retryLater(s, w, now, cfg)
        case _ => Step(s, marks = Vector(Mark.ResolverStaleResult))
    case Event.OutcomeAcknowledged(id, generation) =>
      current(s, id, generation) match
        case Some(Waiting(_, _, _, Phase.Appending(_))) => Step(remove(s, id))
        case _ => Step(s, marks = Vector(Mark.ResolverStaleResult))
    case Event.OutcomeFailed(id, generation) =>
      current(s, id, generation) match
        case Some(w @ Waiting(_, _, _, Phase.Appending(_))) => retryLater(s, w, now, cfg)
        case _ => Step(s, marks = Vector(Mark.ResolverStaleResult))
    case Event.TimeAdvanced => Step(s)

  /** Resolve every straggler not already in flight. Waiting commands are scanned oldest first, up to the first one
    * that is not yet due.
    */
  private def act(s: State, now: ResolverTime, cfg: Config): Step[State, Effect] =
    val ready = s.order.valuesIterator
      .map(s.waiting)
      .takeWhile(isStraggler(_, s, cfg))
      .filter:
        _.phase match
          case Phase.Idle(retryAt) => retryAt.forall(_ <= now)
          case _ => false
      .toVector
    ready.foldLeft(Step[State, Effect](s))((acc, w) => acc.flatMap(resolve(_, w, cfg)))

  private def resolve(s: State, w: Waiting, cfg: Config): Step[State, Effect] =
    val mark = w.command match
      case _: Command.Authorize => Mark.ResolverFencedAuthorization
      case _: Command.Cancel => Mark.ResolverResubmittedCancel
      case _: Command.Capture => Mark.ResolverResubmittedCapture
    val idle = if s.logNow.exists(_ >= w.appended + cfg.deadline + cfg.grace) then Vector.empty else Vector(Mark.ResolverJudgedIdlePartition)
    val cluster = cfg.periods.periodOf(w.command.key.createdAt)
    val submit = Effect.Submit(w.command.id, s.generation, cluster, Transfers.forResolver(w.command, cfg.ledger))
    Step(update(s, w.copy(phase = Phase.Submitting)), Vector(submit), mark +: idle)

  private def commit(s: State): Step[State, Effect] =
    val point = s.order.headOption.fold(s.next)(_._1)
    if point.value > s.committed.value then Step(s.copy(committed = point), Vector(Effect.Commit(s.partition, point)))
    else Step(s)

  private def retryLater(s: State, w: Waiting, now: ResolverTime, cfg: Config): Step[State, Effect] =
    val at = now + cfg.retryBackoff
    Step(update(s, w.copy(phase = Phase.Idle(Some(at)))), Vector(Effect.WakeAt(at)), Vector(Mark.ResolverRetryScheduled))

  private def current(s: State, id: CommandId, generation: Generation): Option[Waiting] =
    if generation == s.generation then s.waiting.get(id) else None

  private def update(s: State, w: Waiting): State = s.copy(waiting = s.waiting.updated(w.command.id, w))

  private def remove(s: State, id: CommandId): State =
    s.waiting.get(id).fold(s)(w => s.copy(waiting = s.waiting - id, order = s.order - w.first))

/** The resolver process: one `ResolverPartition` per assigned partition. On a rebalance it drops the work in
  * flight; reading again from the committed offset rebuilds the state (README section 8, failover). It speaks the
  * ports: the consumer's deliveries come in, and each call and write carries its `Ref`, which the reply carries
  * back to its partition and command.
  */
object Resolver:

  /** A call or a write for one command, on one assignment of its partition. */
  final case class Ref(partition: Partition, id: CommandId, generation: Generation) derives CanEqual

  final case class State(partitions: Map[Partition, ResolverPartition.State], generations: Generation) derives CanEqual

  object State:
    val Initial: State = State(Map.empty, Generation.Initial)

  enum Event derives CanEqual:
    /** Command log: what the consumer reports. */
    case Delivered(delivery: CommandLog.Delivery)
    /** Ledger: the reply to a call. */
    case Returned(ref: Ref, result: Ledger.Result)
    /** Command log: the reply to an outcome's write. */
    case Written(ref: Ref, result: CommandLog.Written)
    /** Clock: a wake-up the resolver asked for. */
    case TimeAdvanced

  enum Effect derives CanEqual:
    case Submit(ref: Ref, submit: Ledger.Submit)
    case Append(ref: Ref, append: CommandLog.Append)
    case Commit(commit: CommandLog.Commit)
    case WakeAt(at: ResolverTime)
    case Raise(alarm: Alarm)

  def machine(cfg: ResolverPartition.Config): Machine[State, Event, Effect, ResolverTime] =
    Machine(State.Initial, step(_, _, _, cfg))

  def step(s: State, now: ResolverTime, e: Event, cfg: ResolverPartition.Config): Step[State, Effect] =
    import CommandLog.Delivery
    import ResolverPartition.Event as P
    e match
      case Event.Delivered(Delivery.Assigned(p, committed)) =>
        val generation = s.generations.next
        Step(State(s.partitions.updated(p, ResolverPartition.assigned(p, generation, committed)), generation))
      case Event.Delivered(Delivery.Revoked(p)) if s.partitions.contains(p) =>
        Step(s.copy(partitions = s.partitions - p), marks = Vector(Mark.ResolverRevoked))
      case Event.Delivered(Delivery.Revoked(_)) => Step(s)
      case Event.Delivered(Delivery.Read(p, offset, appended, record)) => onPartition(s, p, now, P.Read(offset, appended, record), cfg)
      case Event.Delivered(Delivery.ReachedEnd(p)) => onPartition(s, p, now, P.ReachedEnd, cfg)
      case Event.Returned(ref, Ledger.Result.Returned(statuses)) =>
        onPartition(s, ref.partition, now, P.LedgerReturned(ref.id, ref.generation, statuses), cfg)
      case Event.Returned(ref, Ledger.Result.Failed) => onPartition(s, ref.partition, now, P.LedgerFailed(ref.id, ref.generation), cfg)
      case Event.Written(ref, CommandLog.Written.Acknowledged) =>
        onPartition(s, ref.partition, now, P.OutcomeAcknowledged(ref.id, ref.generation), cfg)
      case Event.Written(ref, CommandLog.Written.Failed) => onPartition(s, ref.partition, now, P.OutcomeFailed(ref.id, ref.generation), cfg)
      case Event.TimeAdvanced =>
        s.partitions.keys.toVector.sortBy(_.index).foldLeft(Step[State, Effect](s)): (acc, p) =>
          acc.flatMap(onPartition(_, p, now, P.TimeAdvanced, cfg))

  private def onPartition(
      s: State,
      p: Partition,
      now: ResolverTime,
      event: ResolverPartition.Event,
      cfg: ResolverPartition.Config,
  ): Step[State, Effect] =
    s.partitions.get(p) match
      case None => Step(s)
      case Some(ps) =>
        val next = ResolverPartition.step(ps, now, event, cfg)
        Step(s.copy(partitions = s.partitions.updated(p, next.state)), next.effects.map(lift(p, _)), next.marks)

  private def lift(p: Partition, f: ResolverPartition.Effect): Effect = f match
    case ResolverPartition.Effect.Submit(id, generation, cluster, transfers) =>
      Effect.Submit(Ref(p, id, generation), Ledger.Submit(cluster, transfers))
    case ResolverPartition.Effect.AppendOutcome(id, generation, command, outcome) =>
      Effect.Append(Ref(p, id, generation), CommandLog.Append(Record.OutcomeRecord(command, outcome, Writer.Resolver)))
    case ResolverPartition.Effect.Commit(partition, next) => Effect.Commit(CommandLog.Commit(partition, next))
    case ResolverPartition.Effect.WakeAt(at) => Effect.WakeAt(at)
    case ResolverPartition.Effect.Raise(alarm) => Effect.Raise(alarm)
