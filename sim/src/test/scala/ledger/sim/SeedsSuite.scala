package ledger.sim

import Coverage.Scenario
import ledger.core.*
import munit.ScalaCheckSuite
import org.scalacheck.Prop.forAll

class SeedsSuite extends ScalaCheckSuite:

  private def counted(marks: Iterable[Mark]): MarkCounts =
    val c = new MarkCounts
    c.add(marks)
    c

  private def clean(marks: Iterable[Mark]): RunResult = RunResult(Vector.empty, counted(marks), 1L)

  private val planted = Violation("planted", "a broken invariant")

  test("a run passes when no seed fails and every mark of its scenario is hit"):
    val report = Seeds.run(Scenario.PeriodSwitch, 1L to 3L)(_ => clean(Coverage.required(Scenario.PeriodSwitch)))
    assert(report.passed, report.summary)
    assertEquals(report.runs, 3L)
    assertEquals(report.marks(Mark.EdgePeriodEndedFencePath), 3L)

  test("a run fails, and names the marks, when a mark of its scenario is never hit"):
    val report = Seeds.run(Scenario.PeriodSwitch, 1L to 3L)(_ => clean(Vector(Mark.EdgePeriodEndedFencePath)))
    assert(!report.passed)
    assertEquals(report.missing, Set(Mark.EdgeRefusedPeriodNotStarted))
    assert(report.summary.contains("never hit: EdgeRefusedPeriodNotStarted"), report.summary)

  test("marks of other scenarios are not required"):
    val report = Seeds.run(Scenario.PeriodSwitch, 1L to 1L)(_ => clean(Coverage.required(Scenario.PeriodSwitch)))
    assert(report.passed, report.summary)
    assert(report.marks.hit.intersect(Coverage.required(Scenario.Faults)).isEmpty)

  test("a run stops at the first failing seed, names it, and shows that it replays"):
    var ran = Vector.empty[Long]
    val report = Seeds.run(Scenario.PeriodSwitch, 1L to 100L): seed =>
      ran :+= seed
      if seed == 3L then RunResult(Vector(planted), new MarkCounts, 7L) else clean(Vector.empty)
    assertEquals(report.failure, Some(SeedFailure(3L, Vector(planted), replays = true)))
    assertEquals(report.runs, 3L)
    // The first seed runs twice, and so does the failing one.
    assertEquals(ran, Vector(1L, 1L, 2L, 3L, 3L))
    assert(report.summary.contains("seed 3 failed, and it replays"), report.summary)

  test("a failing seed that does not replay says the simulator is not deterministic"):
    var fingerprint = 0L
    val report = Seeds.run(Scenario.PeriodSwitch, 1L to 1L): _ =>
      fingerprint += 1
      RunResult(Vector(planted), new MarkCounts, fingerprint)
    assertEquals(report.failure.map(_.replays), Some(false))
    assert(report.summary.contains("NOT replay"), report.summary)

  test("a simulator that is not deterministic shows on the first seed, even with no violation"):
    var fingerprint = 0L
    val report = Seeds.run(Scenario.PeriodSwitch, 1L to 100L): _ =>
      fingerprint += 1
      RunResult(Vector.empty, new MarkCounts, fingerprint)
    assertEquals(report.failure, Some(SeedFailure(1L, Vector.empty, replays = false)))
    assertEquals(report.runs, 1L)

  // The parts together, on a small pipeline of their own.

  property("a seed replays exactly: the same trace, the same plan and the same marks"):
    forAll: (seed: Long) =>
      val (a, b) = (Pingers.run(seed), Pingers.run(seed))
      a.result.fingerprint == b.result.fingerprint && a.plan == b.plan && a.result.marks.report == b.result.marks.report

  // A failing seed must replay on any JVM, not only twice in one: nothing in a run may depend on an identity hash
  // code or a hash map's order. A deliberate change to the simulator's parts changes this number.
  test("a seed's run does not change between JVMs"):
    assertEquals(Pingers.run(1L).result.fingerprint, 5594375859678746955L)

  test("different seeds take different runs"):
    assertEquals((1L to 20L).map(Pingers.run(_).result.fingerprint).toSet.size, 20)

  test("a trace can be kept, to read a failing seed's run, and keeping it does not change the run"):
    val (kept, plain) = (Pingers.run(5L, keepTrace = true), Pingers.run(5L))
    assert(kept.trace.nonEmpty)
    assertEquals(kept.result.fingerprint, plain.result.fingerprint)
    assert(kept.trace.exists(_.contains("Inject(Crash")), kept.trace.take(20).mkString("\n"))

  property("a new life never hears an earlier one's replies, whatever the faults"):
    forAll: (seed: Long) =>
      Pingers.run(seed).result.violations.isEmpty

  property("once faults stop every process is up, and every process makes progress again"):
    forAll: (seed: Long) =>
      val run = Pingers.run(seed)
      run.lives.forall(_ == SimProcess.Life.Up) && run.ticksAfterStop.forall(_ > 0)

  test("a planted violation names its seed, and that seed replays"):
    val report = Seeds.run(Scenario.Faults, 1L to 50L)(Pingers.run(_, failOn = Set(9L)).result)
    assertEquals(report.failure.map(_.seed), Some(9L))
    assertEquals(report.failure.map(_.replays), Some(true))
    assert(report.marks(Mark.EdgeAnswered) > 0)
