package ledger.sim

import java.util.concurrent.{Callable, Executors}
import ledger.sim.Coverage.Scenario
import scala.jdk.CollectionConverters.*

/** Runs a range of seeds on every core, one thread to a seed, since each run is one thread with nothing shared. Run
  * with `sim/runMain ledger.sim.RunSeeds <scenario> <first> <last>`.
  */
object RunSeeds:
  private val ChunkSize = 64L

  def run(scenario: Scenario, first: Long, last: Long, threads: Int = Runtime.getRuntime.availableProcessors): SeedsReport =
    val cfg = Pipeline.Config(scenario = scenario)
    val pool = Executors.newFixedThreadPool(threads)
    try
      val chunks = (first to last by ChunkSize).toVector.map: from =>
        val to = (from + ChunkSize - 1).min(last)
        new Callable[SeedsReport]:
          def call(): SeedsReport = Seeds.run(scenario, from to to)(Pipeline.run(_, cfg).result)
      val reports = pool.invokeAll(chunks.asJava).asScala.toVector.map(_.get)
      val marks = new MarkCounts
      reports.foreach(r => marks.addAll(r.marks))
      SeedsReport(scenario, reports.map(_.runs).sum, reports.flatMap(_.failure).headOption, marks)
    finally pool.shutdown()

  def main(args: Array[String]): Unit =
    val (scenario, first, last) = (Scenario.valueOf(args(0)), args(1).toLong, args(2).toLong)
    val started = System.nanoTime
    val report = run(scenario, first, last)
    println(report.summary)
    println(f"${report.runs} seeds in ${(System.nanoTime - started) / 1e9}%.1f s")
    if !report.passed then sys.exit(1)
