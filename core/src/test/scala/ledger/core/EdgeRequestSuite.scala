package ledger.core

import EdgeRequest.{Effect, Event, Origin, Phase}
import Fixtures.*
import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

class EdgeRequestSuite extends ScalaCheckSuite:

  private val k = key(1)
  private val start = EdgeTime(0)

  /** The steps of one request: the time, the event (None for the start) and the effects and marks. */
  private type Trace = Vector[(EdgeTime, Option[Event], Step[EdgeRequest.State, Effect])]

  private def run(request: Request, events: Seq[(Long, Event)], origin: Origin = Origin.Caller, wall: Long = T0, cfg: EdgeRequest.Config = EdgeCfg): Trace =
    val first = EdgeRequest.start(request, origin, start, WallTime(wall), cfg)
    events.foldLeft(Vector[(EdgeTime, Option[Event], Step[EdgeRequest.State, Effect])]((start, None, first))):
      case (trace, (at, e)) => trace :+ ((EdgeTime(at), Some(e), EdgeRequest.step(trace.last._3.state, EdgeTime(at), e, cfg)))

  private def effects(t: Trace): Vector[Effect] = t.flatMap(_._3.effects)
  private def marks(t: Trace): Vector[Mark] = t.flatMap(_._3.marks)
  private def answers(t: Trace): Vector[Reply] = effects(t).collect { case Effect.Answer(r) => r }
  private def submits(t: Trace): Vector[Vector[Transfer]] = effects(t).collect { case Effect.Submit(_, ts) => ts }

  // Example tests: the rows of README sections 4, 5 and 8 that the edge's flow handles.

  test("an authorization: log, call the ledger, log the outcome, then answer"):
    val t = run(authorize(k), Seq(10L -> Event.CommandAcknowledged, 20L -> Event.LedgerReturned(Vector(TbStatus.Created)), 30L -> Event.OutcomeAcknowledged))
    assertEquals(
      effects(t).filterNot(_.isInstanceOf[Effect.WakeAt]),
      Vector(
        Effect.AppendCommand(authCommand(k)),
        Effect.Submit(Period(0), Vector(Transfers.authorization(authCommand(k).asInstanceOf[Command.Authorize], LedgerCfg))),
        Effect.AppendOutcome(authCommand(k), AuthOutcome.Approved),
        Effect.Answer(Reply.Decided(AuthOutcome.Approved)),
      ),
    )
    assertEquals(marks(t), Vector(Mark.AuthorizationCreated, Mark.EdgeAnswered))

  test("section 5: the command log does not acknowledge the command: refused, and the ledger is never called"):
    val t = run(authorize(k), Seq(10L -> Event.CommandFailed))
    assertEquals(answers(t), Vector(Reply.Refused(RefusalReason.CommandNotAcknowledged)))
    assertEquals(submits(t), Vector.empty)

  test("section 5: the command is acknowledged too late for the ledger call and the outcome write: refused"):
    val t = run(authorize(k), Seq(1250L -> Event.CommandAcknowledged))
    assertEquals(answers(t), Vector(Reply.Refused(RefusalReason.AcknowledgedTooLate)))
    assertEquals(submits(t), Vector.empty)
    assert(marks(t).contains(Mark.EdgeRefusedAcknowledgedTooLate))

  test("section 5: no acknowledgement by the hand-off budget: refused, not timed out"):
    val t = run(authorize(k), Seq(1200L -> Event.TimeAdvanced, 1300L -> Event.CommandAcknowledged))
    assertEquals(answers(t), Vector(Reply.Refused(RefusalReason.AcknowledgedTooLate)))
    assertEquals(submits(t), Vector.empty)

  test("section 5: the outcome write fails: answered as on a timeout, and the edge cancels"):
    val t = run(authorize(k), Seq(10L -> Event.CommandAcknowledged, 20L -> Event.LedgerReturned(Vector(TbStatus.Created)), 30L -> Event.OutcomeFailed))
    assertEquals(answers(t), Vector(Reply.TimedOut))
    assert(effects(t).contains(Effect.StartCancel(k)))

  test("section 5: a paused edge finds the resolver's fence: declined, not_executed"):
    val t = run(authorize(k), Seq(10L -> Event.CommandAcknowledged, 20L -> Event.LedgerReturned(Vector(TbStatus.ExistsWithDifferentFlags)), 30L -> Event.OutcomeAcknowledged))
    assertEquals(answers(t), Vector(Reply.Decided(AuthOutcome.Declined(DeclineReason.NotExecuted))))

  test("section 8: at the deadline the authorization times out at once, and the late decision is still logged"):
    val t = run(
      authorize(k),
      Seq(10L -> Event.CommandAcknowledged, 1500L -> Event.TimeAdvanced, 1700L -> Event.LedgerReturned(Vector(TbStatus.Created))),
    )
    assertEquals(answers(t), Vector(Reply.TimedOut))
    assert(effects(t).contains(Effect.StartCancel(k)))
    assert(effects(t).contains(Effect.AppendOutcome(authCommand(k), AuthOutcome.Approved)))
    assert(marks(t).contains(Mark.EdgeLateDecisionLogged))
    assertEquals(t.last._3.state.phase, Phase.Done)

  test("a late result that never comes is abandoned to the resolver"):
    val t = run(authorize(k), Seq(10L -> Event.CommandAcknowledged, 1500L -> Event.TimeAdvanced, 3500L -> Event.TimeAdvanced))
    assert(marks(t).contains(Mark.EdgeLateResultAbandoned))
    assertEquals(effects(t).collect { case e: Effect.AppendOutcome => e }, Vector.empty)

  test("section 8: a capture that times out is retried, never cancelled"):
    val t = run(Request.Capture(k, CaptureAmount.Full), Seq(10L -> Event.CommandAcknowledged, 1500L -> Event.TimeAdvanced))
    assertEquals(answers(t), Vector(Reply.Retry))
    assertEquals(effects(t).collect { case e: Effect.StartCancel => e }, Vector.empty)

  test("section 8: a cancel is one call of the fence and the void, fence first"):
    val t = run(Request.Cancel(k), Seq(10L -> Event.CommandAcknowledged))
    assertEquals(submits(t), Vector(Transfers.cancel(Ids.authorization(k), LedgerCfg)))

  test("section 8: the edge's own cancel answers no one"):
    val t = run(
      Request.Cancel(k),
      Seq(10L -> Event.CommandAcknowledged, 20L -> Event.LedgerReturned(Vector(TbStatus.ExistsWithDifferentFlags, TbStatus.Created)), 30L -> Event.OutcomeAcknowledged),
      origin = Origin.Edge,
    )
    assertEquals(answers(t), Vector.empty)
    assert(effects(t).contains(Effect.AppendOutcome(cancelCommand(k), CancelOutcome.Cancelled)))

  test("section 4: an authorization whose period has not started is refused without writing"):
    val cfg = EdgeCfg.copy(periods = PeriodSchedule(Vector(WallTime(T0 + 1000))))
    val t = run(authorize(key(1, at = T0 + 2000)), Seq.empty, wall = T0, cfg = cfg)
    assertEquals(effects(t), Vector(Effect.Answer(Reply.Refused(RefusalReason.PeriodNotStarted))))

  test("section 4: an authorization whose period has ended is fenced, and approved if a copy created the hold"):
    val cfg = EdgeCfg.copy(periods = PeriodSchedule(Vector(WallTime(T0 + 1000))))
    val late = key(1, at = T0)
    val t = run(
      authorize(late),
      Seq(10L -> Event.CommandAcknowledged, 20L -> Event.LedgerReturned(Vector(TbStatus.ExistsWithDifferentFlags)), 30L -> Event.OutcomeAcknowledged),
      wall = T0 + 5000,
      cfg = cfg,
    )
    assertEquals(submits(t), Vector(Vector(Transfers.fence(Ids.authorization(late), LedgerCfg))))
    assertEquals(answers(t), Vector(Reply.Decided(AuthOutcome.Approved)))
    assert(marks(t).contains(Mark.EdgePeriodEndedFencePath))

  test("a result the rules cannot place: no decision, an alarm, and the edge cancels"):
    val t = run(Request.Cancel(k), Seq(10L -> Event.CommandAcknowledged, 20L -> Event.LedgerReturned(Vector(TbStatus.Created, TbStatus.Created))))
    assertEquals(answers(t), Vector(Reply.Retry))
    assert(effects(t).exists(_.isInstanceOf[Effect.Raise]))

  // Properties over generated event sequences, including late, duplicate and out-of-phase events.

  private def results(kind: Kind): Gen[Vector[TbStatus]] =
    val n = if kind == Kind.Cancel then 2 else 1
    Gen.frequency(9 -> Gen.listOfN(n, Gens.status).map(_.toVector), 1 -> Gen.listOf(Gens.status).map(_.toVector))

  private def event(kind: Kind): Gen[Event] = Gen.frequency(
    3 -> Gen.const(Event.CommandAcknowledged),
    1 -> Gen.const(Event.CommandFailed),
    3 -> results(kind).map(Event.LedgerReturned(_)),
    1 -> Gen.const(Event.LedgerFailed),
    3 -> Gen.const(Event.OutcomeAcknowledged),
    1 -> Gen.const(Event.OutcomeFailed),
    3 -> Gen.const(Event.TimeAdvanced),
  )

  private val request: Gen[Request] = Gen.oneOf(
    Gen.const(authorize(k)),
    Gen.const(Request.Cancel(k)),
    Gens.captureAmount.map(Request.Capture(k, _)),
  )

  private val origin: Gen[Origin] = Gen.frequency(4 -> Gen.const(Origin.Caller), 1 -> Gen.const(Origin.Edge))

  private val traces: Gen[(Request, Origin, Trace)] =
    for
      r <- request
      o <- origin
      n <- Gen.choose(0, 12)
      gaps <- Gen.listOfN(n, Gen.choose(0L, 700L))
      es <- Gen.listOfN(n, event(r.kind))
    yield (r, o, run(r, gaps.scanLeft(0L)(_ + _).tail.zip(es), o))

  property("no ledger call comes before its command's acknowledgement"):
    forAll(traces): (_, _, t) =>
      t.forall((_, e, s) => !s.effects.exists(_.isInstanceOf[Effect.Submit]) || e.contains(Event.CommandAcknowledged))

  property("no authorization reaches the ledger without time left for the call and the outcome write"):
    forAll(traces): (r, _, t) =>
      r.kind != Kind.Authorization ||
      t.forall((at, _, s) => !s.effects.exists(_.isInstanceOf[Effect.Submit]) || at < start + EdgeCfg.handOffBudget)

  property("no decision is answered before its outcome's acknowledgement, nor after the deadline"):
    forAll(traces): (_, _, t) =>
      t.indices.forall: i =>
        val (at, e, s) = t(i)
        s.effects.forall:
          case Effect.Answer(Reply.Decided(o)) =>
            e.contains(Event.OutcomeAcknowledged) && at < start + EdgeCfg.deadline &&
            t.take(i).exists(_._3.effects.contains(Effect.AppendOutcome(initial(t).command, o)))
          case _ => true

  property("every request is answered by its deadline, once at most"):
    forAll(traces): (_, o, t) =>
      val firstDeadlineTick = t.indexWhere((at, e, _) => e.contains(Event.TimeAdvanced) && at >= start + EdgeCfg.deadline)
      val answeredAt = t.indexWhere(_._3.effects.exists(_.isInstanceOf[Effect.Answer]))
      val once = answers(t).size <= 1
      if o == Origin.Edge then answers(t).isEmpty
      else once && (firstDeadlineTick < 0 || (answeredAt >= 0 && answeredAt <= firstDeadlineTick))

  property("a capture's or a cancel's timeout never produces a cancel"):
    forAll(traces): (r, _, t) =>
      r.kind == Kind.Authorization || !effects(t).exists(_.isInstanceOf[Effect.StartCancel])

  property("a refusal is final: nothing reaches the ledger after it"):
    forAll(traces): (_, _, t) =>
      val refusedAt = t.indexWhere(_._3.effects.exists { case Effect.Answer(Reply.Refused(_)) => true; case _ => false })
      refusedAt < 0 || !t.drop(refusedAt).exists(_._3.effects.exists(_.isInstanceOf[Effect.Submit]))

  property("an outcome is logged only from a ledger result"):
    forAll(traces): (_, _, t) =>
      t.forall((_, e, s) => !s.effects.exists(_.isInstanceOf[Effect.AppendOutcome]) || e.exists(_.isInstanceOf[Event.LedgerReturned]))

  property("a finished request does nothing more"):
    forAll(traces): (_, _, t) =>
      val doneAt = t.indexWhere(_._3.state.phase == Phase.Done)
      doneAt < 0 || t.drop(doneAt + 1).forall(_._3.effects.isEmpty)

  test("the generated traces reach every mark of the edge's flow, so the properties are not vacuous"):
    val sample = Gen.listOfN(3000, traces).apply(Gen.Parameters.default, org.scalacheck.rng.Seed(42L)).get
    val hit = sample.flatMap((_, _, t) => marks(t)).toSet
    // The two period marks need a schedule with a switch; the example tests above hit them. The edge process's
    // marks are EdgeSuite's.
    val edgeMarks = Mark.values.toSet.filter(_.toString.startsWith("Edge")) - Mark.EdgeRefusedPeriodNotStarted -
      Mark.EdgePeriodEndedFencePath - Mark.EdgeOwnCancelDropped - Mark.EdgeReplyAfterEnd
    assertEquals(edgeMarks -- hit, Set.empty[Mark])

  private def initial(t: Trace): EdgeRequest.State = t.head._3.state

