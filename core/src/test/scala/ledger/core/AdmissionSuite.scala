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

  private val busy = Admission.State(inFlight = 2, ledgerLatencyMillis = 0, leaderless = Set.empty)
  private val noLeader = Admission.State.Initial.copy(leaderless = Set(partition))

  test("an authorization is admitted and counted in flight"):
    val s = decide(Admission.State.Initial, authorize(k))
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

  test("a slow ledger counts as overload"):
    val slow = (1 to 20).foldLeft(Admission.State.Initial)((s, _) => Admission.step(s, Event.LedgerLatency(400.millis), cfg).state)
    assertEquals(decide(slow, authorize(k)).effects, Vector(Decision.Refuse(RefusalReason.Overload)))

  private val states: Gen[Admission.State] =
    for
      inFlight <- Gen.choose(0, 4)
      latency <- Gen.choose(0.0, 400.0)
      leaderless <- Gen.containerOf[Set, Partition](Gen.choose(0, cfg.partitions - 1).map(Partition(_)))
    yield Admission.State(inFlight, latency, leaderless)

  private val requests: Gen[Request] = Gens.key.flatMap(k =>
    Gen.oneOf(authorize(k), Request.Cancel(k), Request.Capture(k, CaptureAmount.Full))
  )

  property("cancels are never refused"):
    forAll(states, requests): (s, r) =>
      r.kind != Kind.Cancel || !decide(s, r).effects.exists(_.isInstanceOf[Decision.Refuse])

  property("a partition without a leader gets no writes"):
    forAll(states, requests): (s, r) =>
      !s.leaderless(Partitioner.partition(r.a, cfg.partitions)) || !decide(s, r).effects.contains(Decision.Admit)

  property("authorizations are refused once a threshold is crossed"):
    forAll(states, requests): (s, r) =>
      val over = s.inFlight >= cfg.maxInFlight || s.ledgerLatencyMillis >= cfg.maxLedgerLatency.toMillis
      r.kind != Kind.Authorization || !over || decide(s, r).effects.forall(_.isInstanceOf[Decision.Refuse])
