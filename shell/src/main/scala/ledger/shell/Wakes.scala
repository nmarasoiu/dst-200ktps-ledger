package ledger.shell

import cats.effect.IO
import cats.effect.std.Queue
import cats.syntax.all.*
import scala.concurrent.duration.*

/** The wakes a machine asks for, kept in order and served by one sleeping fiber (README section 10, machines and
  * fibers). A fiber asleep per wake is a timer per request, and thousands of timers coming due together can stop Cats
  * Effect's pool for good (typelevel/cats-effect#4674, open in 3.7.1). A wake comes once its time is reached, never
  * before.
  */
final class Wakes[E] private (clock: IO[Long], events: Queue[IO, E], sooner: Queue[IO, Unit]):
  private val pending = new java.util.PriorityQueue[(Long, E)](Ordering.by[(Long, E), Long](_._1))
  // Guarded by `pending`: when the sleeping fiber next wakes, so a wake asked for before then can rouse it.
  private var sleepsUntil = Long.MaxValue

  /** Asks for `event` once `clock` reaches `millis`. */
  def at(millis: Long, event: E): IO[Unit] =
    IO:
      pending.synchronized:
        pending.add((millis, event))
        val earlier = millis < sleepsUntil
        if earlier then sleepsUntil = millis
        earlier
    .flatMap(earlier => sooner.offer(()).whenA(earlier))

  /** Hands over what is due, then sleeps until the next wake or until a sooner one is asked for. */
  def run: IO[Nothing] =
    val round = clock.flatMap: now =>
      val (due, next) = pending.synchronized:
        val due = Vector.newBuilder[E]
        while !pending.isEmpty && pending.peek._1 <= now do due += pending.poll()._2
        sleepsUntil = if pending.isEmpty then Long.MaxValue else pending.peek._1
        (due.result(), sleepsUntil)
      val sleep = if next == Long.MaxValue then IO.never else clock.flatMap(t => IO.sleep((next - t).max(0L).millis))
      due.traverse_(events.offer) >> IO.race(sleep, sooner.take).void
    round.foreverM

object Wakes:
  def apply[E](clock: IO[Long], events: Queue[IO, E]): IO[Wakes[E]] =
    Queue.dropping[IO, Unit](1).map(new Wakes(clock, events, _))
