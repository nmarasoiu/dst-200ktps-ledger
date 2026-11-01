package ledger.sim

import ledger.core.*
import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import scala.concurrent.duration.*

class FaultPlanSuite extends ScalaCheckSuite:

  private val cfg = FaultPlan.Config(HistoryTime(10000), maxGap = 300.millis, maxOutage = 2.seconds, crashChance = 0.5)
  private val targets = Vector("a", "b", "c")
  private def plan(seed: Long) = FaultPlan.generate(new Rng(seed), targets, cfg)

  private val seeds: Gen[Long] = Gen.long

  property("the same seed gives the same plan"):
    forAll(seeds)(seed => plan(seed) == plan(seed))

  property("the plan is in time order, and every fault comes by the time faults stop"):
    forAll(seeds): seed =>
      val times = plan(seed).map(_.at.millis)
      times == times.sorted && times.forall(_ <= cfg.stopAt.millis)

  property("each target has one outage at a time, and is healed by the time faults stop"):
    forAll(seeds): seed =>
      targets.forall: target =>
        val faults = plan(seed).map(_.fault).filter:
          case Fault.Crash(p) => p == target
          case Fault.Restart(p) => p == target
          case Fault.Pause(p) => p == target
          case Fault.Resume(p) => p == target
        faults.size % 2 == 0 && faults.grouped(2).forall:
          case Vector(_: Fault.Crash[String], _: Fault.Restart[String]) => true
          case Vector(_: Fault.Pause[String], _: Fault.Resume[String]) => true
          case _ => false

  test("a plan reaches crashes and pauses of every target"):
    val faults = plan(1L).map(_.fault)
    val crashed = faults.collect { case Fault.Crash(p) => p }.toSet
    val paused = faults.collect { case Fault.Pause(p) => p }.toSet
    assertEquals(crashed, targets.toSet)
    assertEquals(paused, targets.toSet)

  test("different seeds give different plans"):
    assertEquals((1L to 20L).map(plan).toSet.size, 20)
