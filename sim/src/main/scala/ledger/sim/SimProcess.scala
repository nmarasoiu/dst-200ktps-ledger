package ledger.sim

import ledger.core.*

/** One life of a process, from a start to a crash. An opaque `Long`: stored as a primitive. */
opaque type Incarnation = Long

object Incarnation:
  val First: Incarnation = 0L
  extension (i: Incarnation) def next: Incarnation = i + 1
  given CanEqual[Incarnation, Incarnation] = CanEqual.derived

/** A machine run as a process that can crash and pause (README section 11, processes). The simulator performs its
  * effects, and whatever it sent can still land after a crash, as a stale send does. But replies and wake-ups belong
  * to the life that asked for them, so each event is addressed to an incarnation, and a new life never hears an
  * earlier one's.
  */
final class SimProcess[S, E, F, T](machine: Machine[S, E, F, T], read: HistoryTime => T, marks: MarkCounts):
  import SimProcess.Life

  private var current = machine.initial
  private var life = Life.Up
  private var incarnated = Incarnation.First
  private var held = Vector.empty[E]

  def state: S = current
  def status: Life = life
  def incarnation: Incarnation = incarnated

  /** Steps the machine on an event at this time, by the process's own clock. A paused process holds the event until
    * it resumes. A process that is down, or a later life than the one addressed, drops it.
    */
  def deliver(to: Incarnation, at: HistoryTime, e: E): Vector[F] =
    if to != incarnated then Vector.empty
    else
      life match
        case Life.Up => step(at, e)
        case Life.Paused =>
          held :+= e
          Vector.empty
        case Life.Down => Vector.empty

  /** Everything it held is gone, and the next life starts from nothing. */
  def crash(): Unit =
    require(life != Life.Down, "already down")
    life = Life.Down
    incarnated = incarnated.next
    held = Vector.empty
    current = machine.initial

  def restart(): Unit =
    require(life == Life.Down, s"not down: $life")
    life = Life.Up

  def pause(): Unit =
    require(life == Life.Up, s"not up: $life")
    life = Life.Paused

  /** Runs the events it held, in order, by its clock now: this is what makes a zombie. */
  def resume(at: HistoryTime): Vector[F] =
    require(life == Life.Paused, s"not paused: $life")
    life = Life.Up
    val events = held
    held = Vector.empty
    events.flatMap(step(at, _))

  private def step(at: HistoryTime, e: E): Vector[F] =
    val next = machine.step(current, read(at), e)
    current = next.state
    marks.add(next.marks)
    next.effects

object SimProcess:
  enum Life derives CanEqual:
    case Up, Paused, Down
