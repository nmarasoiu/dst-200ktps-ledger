package ledger.sim

import ledger.core.*
import scala.concurrent.duration.FiniteDuration

enum Fault[P] derives CanEqual:
  case Crash(target: P)
  case Restart(target: P)
  case Pause(target: P)
  case Resume(target: P)

final case class TimedFault[P](at: HistoryTime, fault: Fault[P]) derives CanEqual

/** The crashes and pauses of a run, fixed by the seed before the run starts (README section 11, simulator). Each
  * target has one outage at a time, and every outage ends by `stopAt`, so faults stop with every process healed:
  * what liveness mode needs. Steps are atomic, so an outage always falls between two of a process's steps.
  */
object FaultPlan:

  final case class Config(
      /** When faults stop: the history's `faultsStoppedAt`. */
      stopAt: HistoryTime,
      /** The longest gap between one fault and the next. */
      maxGap: FiniteDuration,
      /** The longest a process stays down or paused. */
      maxOutage: FiniteDuration,
      /** The chance that an outage is a crash, not a pause. */
      crashChance: Double,
  )

  def generate[P](rng: Rng, targets: Vector[P], cfg: Config): Vector[TimedFault[P]] =
    val stop = cfg.stopAt.millis
    val busyUntil = new Array[Long](targets.size)
    val plan = Vector.newBuilder[TimedFault[P]]
    var t = rng.between(1, cfg.maxGap.toMillis)
    while t < stop do
      val i = rng.between(0, targets.size - 1).toInt
      if busyUntil(i) < t then
        val end = (t + rng.between(1, cfg.maxOutage.toMillis)).min(stop)
        val (begin, finish) =
          if rng.chance(cfg.crashChance) then (Fault.Crash(targets(i)), Fault.Restart(targets(i)))
          else (Fault.Pause(targets(i)), Fault.Resume(targets(i)))
        plan += TimedFault(HistoryTime(t), begin)
        plan += TimedFault(HistoryTime(end), finish)
        busyUntil(i) = end
      t += rng.between(1, cfg.maxGap.toMillis)
    plan.result().sortBy(_.at.millis)
