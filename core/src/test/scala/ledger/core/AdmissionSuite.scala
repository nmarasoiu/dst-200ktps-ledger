package ledger.core

import Admission.{Decision, Event}
import Fixtures.*
import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import scala.concurrent.duration.*

class AdmissionSuite extends ScalaCheckSuite:

  private val cfg = Admission.Config(maxInFlight = 2, maxLedgerLatency = 200.millis, partitions = 8)
  private val k = key(1)
  private val partition = Partitioner.partition(Ids.authorization(k), cfg.partitions)
  private def decide(s: Admission.State, r: Request): Step[Admission.State, Decision] = Admission.step(s, Event.Arrived(r), cfg)

  private val initial = Admission.State.initial(cfg)
  private val busy = Admission.State(inFlight = 2, limit = 2, leaderless = Set.empty)
  private val noLeader = initial.copy(leaderless = Set(partition))

  test("an authorization is admitted and counted in flight"):
    val s = decide(initial, authorize(k))
    assertEquals(s.effects, Vector(Decision.Admit))
    assertEquals(s.state.inFlight, 1)
    assertEquals(Admission.step(s.state, Event.Finished, cfg).state.inFlight, 0)

  test("a partition without a leader: the authorization is refused at once; cancels and captures retry"):
    assertEquals(decide(noLeader, authorize(k)).effects, Vector(Decision.Refuse(RefusalReason.NoLeader)))
    assertEquals(decide(noLeader, authorize(k)).marks, Vector(Mark.AdmissionRefusedNoLeader))
    assertEquals(decide(noLeader, Request.Cancel(k)).effects, Vector(Decision.Retry))
    assertEquals(decide(noLeader, Request.Cancel(k)).marks, Vector(Mark.AdmissionRetryNoLeader))
    assertEquals(decide(noLeader, Request.Capture(k, CaptureAmount.Full)).effects, Vector(Decision.Retry))

  test("overloaded: the authorization is refused, the capture told to retry, the cancel accepted"):
    assertEquals(decide(busy, authorize(k)).effects, Vector(Decision.Refuse(RefusalReason.Overload)))
    assertEquals(decide(busy, authorize(k)).marks, Vector(Mark.AdmissionRefusedOverload))
    assertEquals(decide(busy, Request.Capture(k, CaptureAmount.Full)).effects, Vector(Decision.Retry))
    assertEquals(decide(busy, Request.Capture(k, CaptureAmount.Full)).marks, Vector(Mark.AdmissionRetryOverload))
    val cancel = decide(busy, Request.Cancel(k))
    assertEquals(cancel.effects, Vector(Decision.Admit))
    assertEquals(cancel.marks, Vector(Mark.AdmissionCancelDuringOverload))

  private def heard(s: Admission.State, latency: FiniteDuration, replies: Int): Admission.State =
    (1 to replies).foldLeft(s)((acc, _) => Admission.step(acc, Event.LedgerLatency(latency), cfg).state)

  test("slow replies lower the limit, down to one, so a slow ledger counts as overload"):
    val slowed = heard(busy.copy(inFlight = 1), 400.millis, 40)
    assertEqualsDouble(slowed.limit, 1.0, 0.0)
    assertEquals(decide(slowed, authorize(k)).effects, Vector(Decision.Refuse(RefusalReason.Overload)))
    assertEquals(decide(slowed, Request.Capture(k, CaptureAmount.Full)).effects, Vector(Decision.Retry))
    assertEquals(decide(slowed, Request.Cancel(k)).marks, Vector(Mark.AdmissionCancelDuringOverload))

  test("past its headroom a cancel is told to retry, with nothing admitted"):
    val full = decide(busy.copy(inFlight = 4), Request.Cancel(k))
    assertEquals(full.effects, Vector(Decision.Retry))
    assertEquals(full.marks, Vector(Mark.AdmissionRetrySaturated))
    assertEquals(full.state.inFlight, 4)

  test("fast replies raise a limit in use, never past the ceiling, and leave an idle one alone"):
    val low = busy.copy(inFlight = 1, limit = 1.0)
    assert(heard(low, 1.milli, 1).limit > 1.0)
    assertEqualsDouble(heard(low, 1.milli, 100).limit, cfg.maxInFlight.toDouble, 0.0)
    val idle = low.copy(inFlight = 0)
    assertEqualsDouble(heard(idle, 1.milli, 100).limit, 1.0, 0.0)

  test("with nothing in flight one request goes, however slow the last replies were"):
    val idle = heard(initial.copy(inFlight = 1), 400.millis, 40).copy(inFlight = 0)
    val first = decide(idle, authorize(k))
    assertEquals(first.effects, Vector(Decision.Admit))
    assertEquals(decide(first.state, authorize(k)).effects, Vector(Decision.Refuse(RefusalReason.Overload)))

  private val states: Gen[Admission.State] =
    for
      inFlight <- Gen.choose(0, 5)
      limit <- Gen.choose(1.0, cfg.maxInFlight.toDouble)
      leaderless <- Gen.containerOf[Set, Partition](Gen.choose(0, cfg.partitions - 1).map(Partition(_)))
    yield Admission.State(inFlight, limit, leaderless)

  private val requests: Gen[Request] = Gens.key.flatMap(k =>
    Gen.oneOf(authorize(k), Request.Cancel(k), Request.Capture(k, CaptureAmount.Full))
  )

  property("no threshold ever leaves an instance with nothing in flight unable to admit, leaders permitting"):
    forAll(states, requests): (s, r) =>
      s.inFlight > 0 || s.leaderless(Partitioner.partition(r.a, cfg.partitions)) || decide(s, r).effects.contains(Decision.Admit)

  property("cancels are never refused"):
    forAll(states, requests): (s, r) =>
      r.kind != Kind.Cancel || !decide(s, r).effects.exists(_.isInstanceOf[Decision.Refuse])

  property("a partition without a leader gets no writes"):
    forAll(states, requests): (s, r) =>
      !s.leaderless(Partitioner.partition(r.a, cfg.partitions)) || !decide(s, r).effects.contains(Decision.Admit)

  property("authorizations are refused once the limit is reached"):
    forAll(states, requests): (s, r) =>
      r.kind != Kind.Authorization || s.inFlight < s.limit || decide(s, r).effects.forall(_.isInstanceOf[Decision.Refuse])

  property("with nothing in flight a request is admitted, wherever its partition has a leader"):
    forAll(states, requests): (s, r) =>
      val idle = s.copy(inFlight = 0)
      idle.leaderless(Partitioner.partition(r.a, cfg.partitions)) || decide(idle, r).effects.contains(Decision.Admit)

  property("a cancel is admitted wherever an authorization or a capture is"):
    forAll(states, Gens.key): (s, k) =>
      val others = Vector[Request](authorize(k), Request.Capture(k, CaptureAmount.Full))
      !others.exists(decide(s, _).effects.contains(Decision.Admit)) || decide(s, Request.Cancel(k)).effects.contains(Decision.Admit)

  property("the limit stays between one and the ceiling, whatever the ledger does"):
    forAll(states, Gen.listOf(Gen.choose(0, 1000))): (s, latencies) =>
      val end = latencies.foldLeft(s)((acc, ms) => Admission.step(acc, Event.LedgerLatency(ms.millis), cfg).state)
      end.limit >= 1.0 && end.limit <= cfg.maxInFlight
