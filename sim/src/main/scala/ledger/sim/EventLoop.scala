package ledger.sim

import java.lang.Long.compare
import java.util.PriorityQueue
import ledger.core.*
import scala.concurrent.duration.FiniteDuration

/** A plain loop over a priority queue of events, ordered by simulated time on the history's clock (README section
  * 11, simulator). Events at one time run in the order they were scheduled, so the order is total and the same seed
  * gives the same run. Events are data, so a trace can print them.
  */
final class EventLoop[E]:
  private final case class Scheduled(at: Long, order: Long, event: E)

  private val queue = new PriorityQueue[Scheduled]((x, y) =>
    val byTime = compare(x.at, y.at)
    if byTime != 0 then byTime else compare(x.order, y.order)
  )
  private var scheduled = 0L
  private var time = 0L

  def now: HistoryTime = HistoryTime(time)

  /** Schedules an event. A time already past means as soon as possible, as for a wake-up whose time has gone by. */
  def schedule(at: HistoryTime, event: E): Unit =
    queue.add(Scheduled(at.millis.max(time), scheduled, event))
    scheduled += 1

  def after(delay: FiniteDuration, event: E): Unit = schedule(now + delay, event)

  /** Runs every event up to and including `until`, then leaves the clock there. Later events stay queued. */
  def run(until: HistoryTime)(handle: E => Unit): Unit =
    while !queue.isEmpty && queue.peek.at <= until.millis do
      val next = queue.poll()
      time = next.at
      handle(next.event)
    time = time.max(until.millis)
