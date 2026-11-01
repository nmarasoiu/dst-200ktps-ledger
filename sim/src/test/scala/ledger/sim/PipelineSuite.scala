package ledger.sim

import ledger.core.*
import ledger.sim.Coverage.Scenario

class PipelineSuite extends munit.FunSuite:
  override def munitTimeout = scala.concurrent.duration.Duration(10, "min")

  test("a seed runs the pipeline to its end and holds every invariant"):
    val r = Pipeline.run(1L)
    assertEquals(r.result.violations.take(5), Vector.empty)
    assert(r.history.caller.nonEmpty)
    assert(r.history.log.nonEmpty)
    assert(r.history.ledger.transfers.nonEmpty)

  test("a run of seeds holds every invariant, and reaches every mark of faults alone"):
    val report = Seeds.run(Scenario.Faults, 1L to 200L)(Pipeline.run(_).result)
    assert(report.passed, report.summary)

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

  // A simulator that never fails proves nothing (README section 14).
  Planted.values.foreach: bug =>
    test(s"a planted bug is caught: $bug"):
      val failing = (1L to 200L).iterator.flatMap: seed =>
        val found = Pipeline.run(seed, planted = Some(bug)).result.violations
        if found.isEmpty then None else Some((seed, found))
      assert(failing.hasNext, s"$bug passed 200 seeds")
      val (seed, found) = failing.next()
      assert(found.exists(_.property == bug.caughtBy), s"seed $seed broke something else: ${found.map(_.property).distinct}")
      // A failure a seed cannot show again is no use to anyone.
      assertEquals(Pipeline.run(seed, planted = Some(bug)).result.violations, found)
