package ledger.core

import Edge.{Effect, Event, Ref}
import Fixtures.*
import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import scala.concurrent.duration.*

class EdgeSuite extends ScalaCheckSuite:

  private val cfg = Edge.Config(EdgeCfg, Admission.Config(maxInFlight = 2, maxLedgerLatency = 200.millis, partitions = 8))
  private val k = key(1)
  private val c1 = Connection(1)
  private val caller = Ref.Caller(c1)
  private val partition = Partitioner.partition(Ids.authorization(k), cfg.admission.partitions)

  private def at(t: Long): Edge.Now = Edge.Now(EdgeTime(t), WallTime(T0 + t))

  /** Each event at its time, from a fresh instance. The first step is the initial state. */
  private def steps(events: (Long, Event)*): Vector[Step[Edge.State, Effect]] =
    events.foldLeft(Vector(Step[Edge.State, Effect](Edge.State.Initial))):
      case (acc, (t, e)) => acc :+ Edge.step(acc.last.state, at(t), e, cfg)

  private def acked(ref: Ref, r: Record): Event = Event.Written(ref, r, CommandLog.Written.Acknowledged)
  private def returned(ref: Ref, statuses: TbStatus*): Event = Event.Returned(ref, Ledger.Result.Returned(statuses.toVector))
  private def append(ref: Ref, r: Record): Effect = Effect.Append(ref, CommandLog.Append(r))
  private def outcome(c: Command, o: Outcome): Record = Record.OutcomeRecord(c, o, Writer.Edge)
  private def withoutWakes(effects: Vector[Effect]): Vector[Effect] = effects.filterNot(_.isInstanceOf[Effect.WakeAt])

  // Example tests: how the request flow and admission control meet (README section 11, processes).

  test("an admitted authorization: log, call the ledger, log the outcome, answer, then leave the count"):
    val auth = authCommand(k)
    val ss = steps(
      0L -> Event.Arrived(c1, authorize(k)),
      10L -> acked(caller, Record.CommandRecord(auth)),
      20L -> returned(caller, TbStatus.Created),
      30L -> acked(caller, outcome(auth, AuthOutcome.Approved)),
    )
    assertEquals(
      withoutWakes(ss.flatMap(_.effects)),
      Vector(
        append(caller, Record.CommandRecord(auth)),
        Effect.Submit(caller, Ledger.Submit(Period(0), Transfers.forEdge(auth, fencePath = false, LedgerCfg))),
        append(caller, outcome(auth, AuthOutcome.Approved)),
        Effect.Answer(c1, Reply.Decided(AuthOutcome.Approved)),
      ),
    )
    assertEquals(ss(1).state.admission.inFlight, 1)
    assert(ss.last.state.requests.isEmpty)
    assertEquals(ss.last.state.admission.inFlight, 0)

  test("overload: an authorization is refused at once with nothing written; a cancel is still admitted"):
    val ss = steps(
      0L -> Event.Arrived(Connection(1), authorize(key(1))),
      0L -> Event.Arrived(Connection(2), authorize(key(2))),
      0L -> Event.Arrived(Connection(3), authorize(key(3))),
      0L -> Event.Arrived(Connection(4), Request.Cancel(key(4))),
    )
    assertEquals(ss(3).effects, Vector(Effect.Answer(Connection(3), Reply.Refused(RefusalReason.Overload))))
    assertEquals(ss(4).effects.collect { case Effect.Append(ref, _) => ref }, Vector[Ref](Ref.Caller(Connection(4))))
    assertEquals(ss.last.state.admission.inFlight, 3)

  test("a partition without a leader: the authorization is refused, the cancel and the capture retry, nothing written"):
    val ss = steps(
      0L -> Event.LeadersChanged(CommandLog.Leaders(Set(partition))),
      0L -> Event.Arrived(Connection(1), authorize(k)),
      0L -> Event.Arrived(Connection(2), Request.Cancel(k)),
      0L -> Event.Arrived(Connection(3), Request.Capture(k, CaptureAmount.Full)),
    )
    assertEquals(
      ss.flatMap(_.effects),
      Vector(
        Effect.Answer(Connection(1), Reply.Refused(RefusalReason.NoLeader)),
        Effect.Answer(Connection(2), Reply.Retry),
        Effect.Answer(Connection(3), Reply.Retry),
      ),
    )

  test("section 8: on its timeout the edge answers timed out and sends its own cancel, which answers no one"):
    val auth = authCommand(k)
    val cancel = cancelCommand(k)
    val own = Ref.OwnCancel(c1)
    val ss = steps(
      0L -> Event.Arrived(c1, authorize(k)),
      10L -> acked(caller, Record.CommandRecord(auth)),
      1500L -> Event.Woke(caller),
      1510L -> acked(own, Record.CommandRecord(cancel)),
      1520L -> returned(own, TbStatus.ExistsWithDifferentFlags, TbStatus.Created),
      1530L -> acked(own, outcome(cancel, CancelOutcome.Cancelled)),
    )
    assertEquals(withoutWakes(ss(3).effects), Vector(Effect.Answer(c1, Reply.TimedOut), append(own, Record.CommandRecord(cancel))))
    // The authorization still waits for its late result, so both count in flight.
    assertEquals(ss(3).state.admission.inFlight, 2)
    assertEquals(ss(5).effects, Vector(append(own, outcome(cancel, CancelOutcome.Cancelled))))
    assertEquals(ss(6).effects, Vector.empty)
    assertEquals(ss(6).state.requests.keySet, Set[Ref](caller))

  test("the edge's own cancel is dropped when its partition has no leader"):
    val ss = steps(
      0L -> Event.Arrived(c1, authorize(k)),
      10L -> acked(caller, Record.CommandRecord(authCommand(k))),
      20L -> Event.LeadersChanged(CommandLog.Leaders(Set(partition))),
      1500L -> Event.Woke(caller),
    )
    assertEquals(withoutWakes(ss(4).effects), Vector(Effect.Answer(c1, Reply.TimedOut)))
    assert(ss(4).marks.contains(Mark.EdgeOwnCancelDropped))
    assertEquals(ss(4).state.admission.inFlight, 1)

  test("the ledger's latency, from each call to its reply, feeds admission control"):
    // Four authorizations in turn, each with a 400 ms ledger call, raise the latency past 200 ms.
    val slow = (1 to 4).flatMap: n =>
      val (c, kn, t) = (Connection(n), key(n), n * 1000L)
      Seq(
        t -> Event.Arrived(c, authorize(kn)),
        (t + 1) -> acked(Ref.Caller(c), Record.CommandRecord(authCommand(kn))),
        (t + 401) -> returned(Ref.Caller(c), TbStatus.Created),
        (t + 402) -> acked(Ref.Caller(c), outcome(authCommand(kn), AuthOutcome.Approved)),
      )
    val ss = steps((slow :+ (9000L -> Event.Arrived(Connection(9), authorize(key(9)))))*)
    assertEquals(ss.flatMap(_.effects).collect { case Effect.Answer(_, r) => r }.take(4), Vector.fill(4)(Reply.Decided(AuthOutcome.Approved)))
    assertEquals(ss.last.effects, Vector(Effect.Answer(Connection(9), Reply.Refused(RefusalReason.Overload))))

  test("a ledger call abandoned without a reply counts at the time it waited"):
    val ss = steps(
      0L -> Event.Arrived(c1, authorize(k)),
      10L -> acked(caller, Record.CommandRecord(authCommand(k))),
      1500L -> Event.Woke(caller),
      3500L -> Event.Woke(caller),
      3600L -> Event.Arrived(Connection(2), authorize(key(2))),
    )
    assert(ss(4).marks.contains(Mark.EdgeLateResultAbandoned))
    assertEquals(ss(5).effects, Vector(Effect.Answer(Connection(2), Reply.Refused(RefusalReason.Overload))))

  test("a reply for a request that has ended changes nothing"):
    val auth = authCommand(k)
    val ss = steps(
      0L -> Event.Arrived(c1, authorize(k)),
      10L -> acked(caller, Record.CommandRecord(auth)),
      1500L -> Event.Woke(caller),
      1600L -> returned(caller, TbStatus.Created),
      1700L -> acked(caller, outcome(auth, AuthOutcome.Approved)),
    )
    // The late decision is logged after the timeout, and its acknowledgement finds the request ended.
    assertEquals(ss(4).effects, Vector(append(caller, outcome(auth, AuthOutcome.Approved))))
    assertEquals(ss(5).effects, Vector.empty)
    assertEquals(ss(5).marks, Vector(Mark.EdgeReplyAfterEnd))

  // Properties over generated runs, with replies coming back in any order.

  private enum Action:
    case Arrive(request: Request)
    case WriteReply(pick: Int, result: CommandLog.Written)
    case LedgerReply(pick: Int, result: Ledger.Result)
    case Wake(pick: Int)
    case Leaders(leaderless: Set[Partition])

  private val requests: Gen[Request] = Gen.choose(1, 4).map(key(_)).flatMap: kn =>
    Gen.oneOf(authorize(kn), Request.Cancel(kn), Request.Capture(kn, CaptureAmount.Full))

  private val actions: Gen[List[(Long, Action)]] =
    val written = Gen.frequency[CommandLog.Written](5 -> Gen.const(CommandLog.Written.Acknowledged), 1 -> Gen.const(CommandLog.Written.Failed))
    val statuses = Gen.choose(1, 2).flatMap(Gen.listOfN(_, Gens.status)).map(ss => Ledger.Result.Returned(ss.toVector))
    val result = Gen.frequency[Ledger.Result](5 -> statuses, 1 -> Gen.const(Ledger.Result.Failed))
    val leaderless = Gen.containerOf[Set, Partition](Gen.choose(0, cfg.admission.partitions - 1).map(Partition(_)))
    val action = Gen.frequency[Action](
      3 -> requests.map(Action.Arrive(_)),
      4 -> Gen.zip(Gen.choose(0, 99), written).map(Action.WriteReply(_, _)),
      3 -> Gen.zip(Gen.choose(0, 99), result).map(Action.LedgerReply(_, _)),
      2 -> Gen.choose(0, 99).map(Action.Wake(_)),
      1 -> leaderless.map(Action.Leaders(_)),
    )
    Gen.choose(0, 40).flatMap(Gen.listOfN(_, Gen.zip(Gen.choose(0L, 600L), action)))

  /** Runs the actions through the edge's machine. Each reply goes to a call or write still without one, picked by
    * index, and a wake-up moves the clock to its time. Returns each step with the state before it.
    */
  private def drive(actions: List[(Long, Action)]): Vector[(Edge.State, Event, Step[Edge.State, Effect])] =
    val m = Edge.machine(cfg)
    var s = m.initial
    var t = 0L
    var connections = 0L
    var writes = Vector.empty[Effect.Append]
    var calls = Vector.empty[Effect.Submit]
    var wakes = Vector.empty[Effect.WakeAt]
    val out = Vector.newBuilder[(Edge.State, Event, Step[Edge.State, Effect])]
    def take[A](xs: Vector[A], i: Int): Option[(A, Vector[A])] = Option.when(xs.nonEmpty)((xs(i % xs.size), xs.patch(i % xs.size, Nil, 1)))
    actions.foreach: (gap, action) =>
      t += gap
      val event = action match
        case Action.Arrive(r) =>
          connections += 1
          Some(Event.Arrived(Connection(connections), r))
        case Action.WriteReply(i, result) =>
          take(writes, i).map: (w, rest) =>
            writes = rest
            Event.Written(w.ref, w.append.record, result)
        case Action.LedgerReply(i, result) =>
          take(calls, i).map: (c, rest) =>
            calls = rest
            Event.Returned(c.ref, result)
        case Action.Wake(i) =>
          take(wakes, i).map: (w, rest) =>
            wakes = rest
            t = t.max(w.at.millis)
            Event.Woke(w.ref)
        case Action.Leaders(leaderless) => Some(Event.LeadersChanged(CommandLog.Leaders(leaderless)))
      event.foreach: e =>
        val st = m.step(s, at(t), e)
        out += ((s, e, st))
        s = st.state
        st.effects.foreach:
          case w: Effect.Append => writes :+= w
          case c: Effect.Submit => calls :+= c
          case w: Effect.WakeAt => wakes :+= w
          case _ => ()
    out.result()

  property("admission control counts exactly the requests in flight"):
    forAll(actions): as =>
      drive(as).forall((_, _, st) => st.state.admission.inFlight == st.state.requests.size)

  property("a request whose partition has no leader is answered at once, with nothing written"):
    forAll(actions): as =>
      drive(as).forall:
        case (before, Event.Arrived(_, r), st) if before.admission.leaderless(Partitioner.partition(r.a, cfg.admission.partitions)) =>
          !st.effects.exists(_.isInstanceOf[Effect.Append]) && st.effects.exists(_.isInstanceOf[Effect.Answer])
        case _ => true

  property("each connection that arrived is answered at most once, and no other is"):
    forAll(actions): as =>
      val run = drive(as)
      val arrived = run.collect { case (_, Event.Arrived(c, _), _) => c }.toSet
      val answered = run.flatMap(_._3.effects).collect { case Effect.Answer(c, _) => c }
      answered.distinct.size == answered.size && answered.forall(arrived)

  test("the generated runs reach the edge process's marks, so the properties are not vacuous"):
    val sample = Gen.listOfN(500, actions).apply(Gen.Parameters.default, org.scalacheck.rng.Seed(42L)).get
    val hit = sample.flatMap(drive(_).flatMap(_._3.marks)).toSet
    val expected = Set(
      Mark.EdgeOwnCancelDropped,
      Mark.EdgeReplyAfterEnd,
      Mark.EdgeTimedOutAuthorization,
      Mark.AdmissionRefusedOverload,
      Mark.AdmissionRefusedNoLeader,
    )
    assertEquals(expected -- hit, Set.empty[Mark])
