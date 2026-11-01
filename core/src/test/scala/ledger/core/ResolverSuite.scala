package ledger.core

import Fixtures.*
import ResolverPartition.{Effect, Event}
import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

class ResolverSuite extends ScalaCheckSuite:

  private val p0 = Partition(0)
  private val auth = authCommand(key(1))
  private val other = authCommand(key(2))

  private def step(s: ResolverPartition.State, now: Long, e: Event): Step[ResolverPartition.State, Effect] =
    ResolverPartition.step(s, ResolverTime(now), e, ResolverCfg)

  private def steps(events: (Long, Event)*): Vector[Step[ResolverPartition.State, Effect]] =
    events.foldLeft(Vector(Step[ResolverPartition.State, Effect](ResolverPartition.assigned(p0, Generation(1), Offset(0))))):
      case (acc, (now, e)) => acc :+ step(acc.last.state, now, e)

  private def read(offset: Long, at: Long, r: Record): (Long, Event) = at -> Event.Read(Offset(offset), LogTime(at), r)
  private def cmd(c: Command) = Record.CommandRecord(c)
  private def out(c: Command, o: Outcome) = Record.OutcomeRecord(c, o, Writer.Edge)
  private def submits(ss: Vector[Step[?, Effect]]): Vector[Effect.Submit] = ss.flatMap(_.effects).collect { case s: Effect.Submit => s }

  // Example tests: README section 5, crash rows, and section 8, resolver.

  test("section 5: a command left without an outcome is fenced once a record past its due time is read"):
    val ss = steps(read(0, 0, cmd(auth)), read(1, Due - 1, cmd(other)), read(2, Due, out(other, AuthOutcome.Approved)))
    assertEquals(submits(ss), Vector(Effect.Submit(auth.id, Generation(1), Period(0), Vector(Transfers.fence(auth.a, LedgerCfg)))))
    assert(ss.last.marks.contains(Mark.ResolverFencedAuthorization))
    val resolved = steps(
      read(0, 0, cmd(auth)),
      read(1, Due, out(other, AuthOutcome.Approved)),
      Due -> Event.LedgerReturned(auth.id, Generation(1), Vector(TbStatus.ExistsWithDifferentFlags)),
      Due -> Event.OutcomeAcknowledged(auth.id, Generation(1)),
    )
    assert(resolved(3).effects.contains(Effect.AppendOutcome(auth.id, Generation(1), auth, AuthOutcome.Approved)))
    assertEquals(resolved.last.effects, Vector(Effect.Commit(p0, Offset(2))))

  test("section 5: an edge's outcome read within the grace period: no action, and the commit moves past it"):
    val ss = steps(read(0, 0, cmd(auth)), read(1, 100, out(auth, AuthOutcome.Approved)), read(2, Due + 1, cmd(other)))
    assertEquals(submits(ss), Vector.empty)
    assertEquals(ss(2).effects, Vector(Effect.Commit(p0, Offset(2))))
    assert(ss(2).marks.contains(Mark.ResolverFoundOutcome))

  test("the commit never passes the oldest command without an outcome"):
    val ss = steps(read(0, 0, cmd(auth)), read(1, 10, cmd(other)), read(2, 20, out(other, AuthOutcome.Approved)))
    assertEquals(ss.flatMap(_.effects).collect { case c: Effect.Commit => c }, Vector.empty)

  test("an idle partition is judged by the resolver's clock, once its end is reached past the due time"):
    val early = steps(read(0, 0, cmd(auth)), (Due - 1) -> Event.ReachedEnd)
    assertEquals(submits(early), Vector.empty)
    val late = steps(read(0, 0, cmd(auth)), Due -> Event.ReachedEnd)
    assertEquals(submits(late).map(_.id), Vector(auth.id))
    assert(late.last.marks.contains(Mark.ResolverJudgedIdlePartition))

  test("an end reached before the due time does not count later: the clock alone never judges"):
    val ss = steps(read(0, 0, cmd(auth)), 100L -> Event.ReachedEnd, (Due + 60_000) -> Event.TimeAdvanced)
    assertEquals(submits(ss), Vector.empty)

  test("section 5: an outcome read while the fence is in flight: the late result is ignored, no duplicate"):
    val ss = steps(
      read(0, 0, cmd(auth)),
      Due -> Event.ReachedEnd,
      read(1, Due + 5, out(auth, AuthOutcome.Approved)),
      (Due + 6) -> Event.LedgerReturned(auth.id, Generation(1), Vector(TbStatus.ExistsWithDifferentFlags)),
    )
    assert(ss(3).marks.contains(Mark.ResolverOutcomeWhileInFlight))
    assertEquals(ss.flatMap(_.effects).collect { case a: Effect.AppendOutcome => a }, Vector.empty)
    assertEquals(ss.last.marks, Vector(Mark.ResolverStaleResult))

  test("section 8: a straggling cancel is resubmitted as the fence and the void; a capture as its post"):
    val cancel = cancelCommand(key(3))
    val capture = captureCommand(key(4), CaptureAmount.Partial(5))
    val ss = steps(read(0, 0, cmd(cancel)), read(1, 0, cmd(capture)), Due -> Event.ReachedEnd)
    assertEquals(submits(ss).map(_.transfers), Vector(Transfers.cancel(cancel.a, LedgerCfg), Vector(Transfers.post(capture.a, CaptureAmount.Partial(5)))))

  test("a result the rules cannot place is retried after the backoff, with an alarm"):
    val ss = steps(
      read(0, 0, cmd(auth)),
      Due -> Event.ReachedEnd,
      Due -> Event.LedgerReturned(auth.id, Generation(1), Vector(TbStatus.LedgerMustNotBeZero)),
      (Due + 100) -> Event.TimeAdvanced,
      (Due + 500) -> Event.TimeAdvanced,
    )
    assert(ss(3).effects.exists(_.isInstanceOf[Effect.Raise]))
    assert(ss(3).effects.contains(Effect.WakeAt(ResolverTime(Due + 500))))
    assertEquals(submits(Vector(ss(4))), Vector.empty)
    assertEquals(submits(Vector(ss(5))).map(_.id), Vector(auth.id))

  test("records read again below the next offset are ignored"):
    val ss = steps(read(0, 0, cmd(auth)), read(0, 0, cmd(auth)))
    assertEquals(ss.last.marks, Vector(Mark.ResolverRedelivered))

  test("section 5: a zombie's results, from before a rebalance, are dropped"):
    import CommandLog.Delivery
    import Resolver.Event.Delivered
    val cfg = ResolverCfg
    val now = ResolverTime(Due)
    val assigned = Resolver.step(Resolver.State.Initial, now, Delivered(Delivery.Assigned(p0, Offset(0))), cfg)
    val first = Seq(
      Delivered(Delivery.Read(p0, Offset(0), LogTime(0), cmd(auth))),
      Delivered(Delivery.ReachedEnd(p0)),
      Delivered(Delivery.Revoked(p0)),
      Delivered(Delivery.Assigned(p0, Offset(0))),
      Resolver.Event.Returned(Resolver.Ref(p0, auth.id, Generation(1)), Ledger.Result.Returned(Vector(TbStatus.Created))),
    ).scanLeft(assigned)((s, e) => Resolver.step(s.state, now, e, cfg))
    assertEquals(first(2).effects.collect { case Resolver.Effect.Submit(ref, _) => ref.generation }, Vector(Generation(1)))
    assertEquals(first(3).marks, Vector(Mark.ResolverRevoked))
    assertEquals(first.last.effects, Vector.empty)
    assertEquals(first.last.marks, Vector(Mark.ResolverStaleResult))

  test("the process speaks the ports: a fence goes out with its ref, and its reply comes back through it"):
    import CommandLog.Delivery
    import Resolver.Event.Delivered
    val ref = Resolver.Ref(p0, auth.id, Generation(1))
    val m = Resolver.machine(ResolverCfg)
    val ss = Seq(
      Delivered(Delivery.Assigned(p0, Offset(0))),
      Delivered(Delivery.Read(p0, Offset(0), LogTime(0), cmd(auth))),
      Delivered(Delivery.ReachedEnd(p0)),
      Resolver.Event.Returned(ref, Ledger.Result.Returned(Vector(TbStatus.Created))),
      Resolver.Event.Written(ref, CommandLog.Written.Acknowledged),
    ).scanLeft(Step[Resolver.State, Resolver.Effect](m.initial))((s, e) => m.step(s.state, ResolverTime(Due), e))
    assertEquals(ss(3).effects, Vector(Resolver.Effect.Submit(ref, Ledger.Submit(Period(0), Vector(Transfers.fence(auth.a, LedgerCfg))))))
    val decided = Record.OutcomeRecord(auth, AuthOutcome.Declined(DeclineReason.NotExecuted), Writer.Resolver)
    assertEquals(ss(4).effects, Vector(Resolver.Effect.Append(ref, CommandLog.Append(decided))))
    assertEquals(ss(5).effects, Vector(Resolver.Effect.Commit(CommandLog.Commit(p0, Offset(1)))))

  // Properties over generated logs, with a small driver that answers the resolver's calls and writes.

  private final case class Entry(appended: Long, record: Record)

  private def commandOf(n: Int, kind: Kind): Command = kind match
    case Kind.Authorization => authCommand(key(n))
    case Kind.Cancel => cancelCommand(key(n))
    case Kind.Capture => captureCommand(key(n))

  private def outcomeOf(kind: Kind): Outcome = kind match
    case Kind.Authorization => AuthOutcome.Approved
    case Kind.Cancel => CancelOutcome.Cancelled
    case Kind.Capture => CaptureOutcome.Captured(CaptureAmount.Full)

  /** Commands on one partition, each with an outcome that may come after `maxDelay` or never. */
  private def logs(maxDelay: Long, alwaysOutcome: Boolean): Gen[Vector[Entry]] =
    val command = for
      n <- Gen.choose(1, 4)
      kind <- Gen.oneOf(Kind.values.toSeq)
      gap <- Gen.choose(0L, 2000L)
      delay <- if alwaysOutcome then Gen.some(Gen.choose(0L, maxDelay)) else Gen.option(Gen.choose(0L, maxDelay))
    yield (commandOf(n, kind), gap, delay)
    Gen.choose(1, 15).flatMap(Gen.listOfN(_, command)).map: cs =>
      val times = cs.map(_._2).scanLeft(0L)(_ + _).tail
      cs.zip(times)
        .flatMap { case ((c, _, delay), t) => Entry(t, Record.CommandRecord(c)) +: delay.toList.map(d => Entry(t + d, Record.OutcomeRecord(c, outcomeOf(c.kind), Writer.Edge))) }
        .sortBy(_.appended)
        .toVector

  private final case class Driven(effects: Vector[Effect], marks: Vector[Mark], commitsPastWaiting: Vector[String], end: ResolverPartition.State)

  /** Reads the log from `from`, then reaches its end long after the last record. A reference model tracks the
    * commands still without an outcome, to check every commit against.
    */
  private def drive(log: Vector[Entry], from: Int, answer: Command => Vector[TbStatus]): Driven =
    val commands = log.map(_.record.command).map(c => c.id -> c).toMap
    var s = ResolverPartition.assigned(p0, Generation(1), Offset(from))
    var waiting = Map.empty[CommandId, Long]
    val effects = Vector.newBuilder[Effect]
    val marks = Vector.newBuilder[Mark]
    val broken = Vector.newBuilder[String]
    def process(now: Long, e: Event): Unit =
      e match
        case Event.Read(o, _, Record.CommandRecord(c)) if !waiting.contains(c.id) => waiting += c.id -> o.value
        case Event.Read(_, _, Record.OutcomeRecord(c, _, _)) => waiting -= c.id
        case Event.OutcomeAcknowledged(id, _) => waiting -= id
        case _ => ()
      val st = step(s, now, e)
      s = st.state
      effects ++= st.effects
      marks ++= st.marks
      st.effects.foreach:
        case Effect.Commit(_, next) if waiting.values.exists(_ < next.value) => broken += s"commit $next passes $waiting"
        case _ => ()
      st.effects.foreach:
        case Effect.Submit(id, g, _, _) => process(now, Event.LedgerReturned(id, g, answer(commands(id))))
        case Effect.AppendOutcome(id, g, _, _) => process(now, Event.OutcomeAcknowledged(id, g))
        case _ => ()
    log.zipWithIndex.drop(from).foreach((entry, i) => process(entry.appended, Event.Read(Offset(i), LogTime(entry.appended), entry.record)))
    process(log.lastOption.fold(0L)(_.appended) + Due + 10_000, Event.ReachedEnd)
    Driven(effects.result(), marks.result(), broken.result(), s)

  private def wellFormed(c: Command): Vector[TbStatus] = c match
    case _: Command.Authorize => Vector(TbStatus.Created)
    case _: Command.Cancel => Vector(TbStatus.Created, TbStatus.PendingTransferNotPending)
    case _: Command.Capture => Vector(TbStatus.Created)

  private val anyAnswers: Gen[Command => Vector[TbStatus]] =
    Gen.listOfN(3, Gens.status).map(ss => c => if c.kind == Kind.Cancel then ss.take(2).toVector else Vector(ss((c.a.value.lo & 1).toInt)))

  property("the resolver never submits an authorization: it fences it"):
    forAll(logs(8000, alwaysOutcome = false), anyAnswers): (log, answer) =>
      drive(log, 0, answer).effects.forall:
        case Effect.Submit(_, _, _, ts) => !ts.exists(_.isPending)
        case _ => true

  property("no offset commit passes a command without an outcome"):
    forAll(logs(8000, alwaysOutcome = false), anyAnswers): (log, answer) =>
      drive(log, 0, answer).commitsPastWaiting.isEmpty

  property("every straggler gets an outcome once its grace period has passed in log time"):
    forAll(logs(8000, alwaysOutcome = false)): log =>
      val driven = drive(log, 0, wellFormed)
      val written = driven.effects.collect { case Effect.AppendOutcome(id, _, _, _) => id }.toSet
      val stragglers = log.zipWithIndex.collect:
        case (Entry(_, Record.CommandRecord(c)), i)
            if !log.drop(i + 1).exists(e => e.record.isInstanceOf[Record.OutcomeRecord] && e.record.command.id == c.id) =>
          c.id
      stragglers.forall(written) && driven.end.waiting.isEmpty && driven.end.committed == Offset(log.size)

  property("a resolver reading again from any offset acts only on commands with no outcome in the log"):
    forAll(logs(Due - 1, alwaysOutcome = true).flatMap(log => Gen.choose(0, log.size).map(log -> _)), anyAnswers):
      case ((log, from), answer) => drive(log, from, answer).effects.forall(!_.isInstanceOf[Effect.Submit])

  test("the generated logs reach the resolver's marks, so the properties are not vacuous"):
    val sample = Gen
      .listOfN(300, Gen.zip(logs(8000, alwaysOutcome = false), anyAnswers))
      .apply(Gen.Parameters.default, org.scalacheck.rng.Seed(7L))
      .get
    val hit = sample.flatMap((log, answer) => drive(log, 0, answer).marks).toSet
    val expected = Set(
      Mark.ResolverFencedAuthorization,
      Mark.ResolverResubmittedCancel,
      Mark.ResolverResubmittedCapture,
      Mark.ResolverJudgedIdlePartition,
      Mark.ResolverFoundOutcome,
      Mark.ResolverRetryScheduled,
    )
    assertEquals(expected -- hit, Set.empty[Mark])
