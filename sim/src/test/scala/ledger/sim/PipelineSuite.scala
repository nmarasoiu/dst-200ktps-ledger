package ledger.sim

import ledger.core.*
import ledger.sim.Coverage.Scenario
import scala.concurrent.duration.*

class PipelineSuite extends munit.FunSuite:
  override def munitTimeout: Duration = 10.minutes

  test("a seed runs the pipeline to its end and holds every invariant"):
    val r = Pipeline.run(1L)
    assertEquals(r.result.violations.take(5), Vector.empty)
    assert(r.history.caller.nonEmpty)
    assert(r.history.log.nonEmpty)
    assert(r.history.ledger.transfers.nonEmpty)

  test("a run of seeds holds every invariant, and reaches every mark of faults alone"):
    val report = Seeds.run(Scenario.Faults, 1L to 200L)(Pipeline.run(_).result)
    assert(report.passed, report.summary)

  List(Scenario.ContractBreakingCaller, Scenario.MisconfiguredLedger, Scenario.PeriodSwitch, Scenario.Overload).foreach: scenario =>
    test(s"a run of seeds holds every invariant, and reaches every mark of its scenario: $scenario"):
      val report = Seeds.run(scenario, 1L to 300L)(Pipeline.run(_, Pipeline.Config(scenario = scenario)).result)
      assert(report.passed, report.summary.linesIterator.take(1).mkString)

  // Safety must hold under any skew; only timeliness may suffer (README section 11, simulator).
  test("clocks minutes apart break no safety invariant"):
    (1L to 100L).foreach: seed =>
      val history = Pipeline.run(seed, Pipeline.Config(maxSkew = 5.minutes)).history
      assertEquals(Invariants.checkKeptContracts(history), Vector.empty, s"seed $seed")

  test("a seed replays exactly"):
    val (a, b) = (Pipeline.run(7L), Pipeline.run(7L))
    assertEquals(a.result.fingerprint, b.result.fingerprint)
    assertEquals(a.history, b.history)

  test("different seeds take different runs"):
    assertEquals((1L to 10L).map(Pipeline.run(_).result.fingerprint).toSet.size, 10)

  test("a healed pipeline approves again: the run does not end stuck refusing"):
    (1L to 20L).foreach: seed =>
      val r = Pipeline.run(seed)
      val calm = r.history.caller.collect:
        case CallerRecord.Received(at, _, Reply.Decided(AuthOutcome.Approved)) if at >= Pipeline.Config().faults.stopAt + Pipeline.bounds.settle => at
      assert(calm.nonEmpty, s"seed $seed approved nothing once healed")

  /** The bugs of admission control show only under load, the second only with a threshold set too high. */
  private def provokedBy(bug: Planted): Pipeline.Config = bug match
    case Planted.EdgeAdmitsEverything => Pipeline.Config(scenario = Scenario.Overload)
    case Planted.EdgeAcceptsEveryCancel => hotStart
    case _ => Pipeline.Config()

  // An instance that starts far above what the ledger can serve, so the queue outruns the deadline before any reply
  // says so. Nothing else makes so many cancels wait at once.
  private val hotStart =
    Pipeline.Config(scenario = Scenario.Overload, overload = Pipeline.Overload(floorShare = 0), maxInFlight = 200, startInFlight = 200)

  test("the limit finds what the ledger can serve, however high its ceiling is"):
    (1L to 30L).foreach: seed =>
      val r = Pipeline.run(seed, Pipeline.Config(scenario = Scenario.Overload, maxInFlight = 5000))
      assertEquals(r.result.violations.take(3), Vector.empty, s"seed $seed")

  test("a queue that outran the deadline costs throughput for a while, but drains, and every cancel is acknowledged"):
    (1L to 30L).foreach: seed =>
      assertEquals(Pipeline.run(seed, hotStart).result.violations.take(3), Vector.empty, s"seed $seed")

  // A simulator that never fails proves nothing (README section 14).
  Planted.values.foreach: bug =>
    test(s"a planted bug is caught: $bug"):
      val failing = (1L to 200L).iterator.flatMap: seed =>
        val found = Pipeline.run(seed, provokedBy(bug), planted = Some(bug)).result.violations
        if found.isEmpty then None else Some((seed, found))
      assert(failing.hasNext, s"$bug passed 200 seeds")
      val (seed, found) = failing.next()
      assert(found.exists(_.property == bug.caughtBy), s"seed $seed broke something else: ${found.map(_.property).distinct}")
      // A failure a seed cannot show again is no use to anyone.
      assertEquals(Pipeline.run(seed, provokedBy(bug), planted = Some(bug)).result.violations, found)
