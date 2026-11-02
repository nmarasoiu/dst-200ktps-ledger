package ledger.shell

import cats.effect.IO
import cats.effect.std.Queue
import cats.syntax.all.*
import ledger.core.*

/** The shell makes no decisions (README section 11, core as pure state machines). One fiber takes one event at a time,
  * reads the clock and steps the machine, so a machine needs no locks and sees events in one order. `perform` must not
  * wait for an effect to finish: what an effect comes to arrives later, as another event.
  */
object MachineLoop:
  /** What sits in front of a machine, so one that falls behind holds back its sources rather than growing a queue. */
  val EventCapacity = 4096

  def run[S, E, F, T](machine: Machine[S, E, F, T], now: IO[T], events: Queue[IO, E], perform: F => IO[Unit]): IO[Nothing] =
    def go(s: S): IO[Nothing] =
      events.take
        .flatMap(e => now.map(machine.step(s, _, e)))
        .flatMap(next => next.effects.traverse_(perform).as(next.state))
        .flatMap(go)
    go(machine.initial)
