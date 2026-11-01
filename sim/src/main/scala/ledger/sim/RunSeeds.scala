package ledger.sim

import java.util.concurrent.{Callable, Executors}
import ledger.sim.Coverage.Scenario
import scala.jdk.CollectionConverters.*

/** Runs a range of seeds on every core, one thread to a seed, since each run is one thread with nothing shared. Run
  * with `sim/runMain ledger.sim.RunSeeds <first> <last>`.
  */
object RunSeeds:
  private val ChunkSize = 64L

  def run(first: Long, last: Long, threads: Int = Runtime.getRuntime.availableProcessors): SeedsReport =
    val pool = Executors.newFixedThreadPool(threads)
    try
      val chunks = (first to last by ChunkSize).toVector.map: from =>
        val to = (from + ChunkSize - 1).min(last)
        new Callable[SeedsReport]:
          def call(): SeedsReport = Seeds.run(Scenario.Faults, from to to)(Pipeline.run(_).result)
      val reports = pool.invokeAll(chunks.asJava).asScala.toVector.map(_.get)
      val marks = new MarkCounts
      reports.foreach(r => marks.addAll(r.marks))
      SeedsReport(Scenario.Faults, reports.map(_.runs).sum, reports.flatMap(_.failure).headOption, marks)
    finally pool.shutdown()

  def main(args: Array[String]): Unit =
    val (first, last) = (args(0).toLong, args(1).toLong)
    val started = System.nanoTime
    val report = run(first, last)
    println(report.summary)
    println(f"${report.runs} seeds in ${(System.nanoTime - started) / 1e9}%.1f s")
    if !report.passed then sys.exit(1)
