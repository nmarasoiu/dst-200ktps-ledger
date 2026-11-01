package ledger.sim

import ledger.core.*
import ledger.sim.Coverage.Scenario

/** What one seed's run found. */
final case class RunResult(violations: Vector[Violation], marks: MarkCounts, fingerprint: Long)

/** A seed that broke something, or that gave a different run the second time. `replays` says whether running it
  * again gave the same run; if not, the simulator itself is not deterministic.
  */
final case class SeedFailure(seed: Long, violations: Vector[Violation], replays: Boolean) derives CanEqual

final case class SeedsReport(scenario: Scenario, runs: Long, failure: Option[SeedFailure], marks: MarkCounts):
  /** The marks of this scenario that no run hit: evidence that the hard cases were not reached. */
  def missing: Set[Mark] = Coverage.required(scenario).filter(marks(_) == 0)
  def passed: Boolean = failure.isEmpty && missing.isEmpty

  def summary: String =
    val found = failure.fold("no failing seed"): f =>
      val again = if f.replays then "and it replays" else "and it does NOT replay: the simulator is not deterministic"
      s"seed ${f.seed} failed, $again: ${f.violations.mkString("; ")}"
    val unreached = if missing.isEmpty then "every mark of the scenario hit" else s"never hit: ${missing.map(_.toString).toVector.sorted.mkString(", ")}"
    s"$scenario, $runs runs: $found; $unreached\n${marks.report}"

/** Runs seeds until one fails (README section 11, simulator). The first seed runs twice, so a simulator that is not
  * deterministic shows at once, and a failing seed runs again to show that it replays.
  */
object Seeds:
  def run(scenario: Scenario, seeds: Iterable[Long])(one: Long => RunResult): SeedsReport =
    val marks = new MarkCounts
    var runs = 0L
    var failure = Option.empty[SeedFailure]
    val remaining = seeds.iterator
    while failure.isEmpty && remaining.hasNext do
      val seed = remaining.next()
      val result = one(seed)
      marks.addAll(result.marks)
      runs += 1
      if result.violations.nonEmpty || runs == 1 then
        val again = one(seed)
        val replays = again.fingerprint == result.fingerprint && again.violations == result.violations
        if result.violations.nonEmpty || !replays then failure = Some(SeedFailure(seed, result.violations, replays))
    SeedsReport(scenario, runs, failure, marks)
