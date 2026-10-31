package ledger.core

import java.util.concurrent.TimeUnit.MILLISECONDS
import scala.concurrent.duration.FiniteDuration

/** Whose clock a time was read from. Times from different clocks cannot be compared, so safety cannot come to
  * depend on two clocks agreeing (README section 8, clocks).
  */
sealed trait Clock

object Clock:
  /** The edge's monotonic clock, which times the 1.5 s deadline. */
  sealed trait Edge extends Clock
  /** A partition leader's clock, stamped on each record as its append time. */
  sealed trait Log extends Clock
  /** The resolver's own clock, used only for a partition with no newer records. */
  sealed trait Resolver extends Clock
  /** Calendar time in epoch milliseconds, as a wall clock reads it: the caller's, for a key's creation time; an
    * edge's, to place a key's period; and the period schedule itself. These are compared on purpose, and the fence
    * keeps clocks that disagree safe (README section 4, one cluster per period).
    */
  sealed trait Wall extends Clock
  /** The one clock of a history: the simulator's own time, or the workload driver's clock in real runs. */
  sealed trait History extends Clock

/** A time in milliseconds, read from clock `C`. An opaque `Long`: stored as a primitive, while times from different
  * clocks stay different types.
  */
opaque type At[C <: Clock] = Long

object At:
  def apply[C <: Clock](millis: Long): At[C] = millis

  given [C <: Clock] => Ordering[At[C]] = Ordering.Long
  given [C <: Clock] => CanEqual[At[C], At[C]] = CanEqual.derived

  extension [C <: Clock](t: At[C])
    def millis: Long = t
    def +(d: FiniteDuration): At[C] = t + d.toMillis
    /** The time between two readings of the same clock. */
    def -(that: At[C]): FiniteDuration = FiniteDuration(t - that, MILLISECONDS)
    def <(that: At[C]): Boolean = t < that
    def <=(that: At[C]): Boolean = t <= that
    def >(that: At[C]): Boolean = t > that
    def >=(that: At[C]): Boolean = t >= that

  extension (t: At[Clock.Log])
    /** The one place two clocks meet: an idle partition, judged by the resolver's clock (README section 8,
      * stragglers). Skew there changes only how soon the resolver acts.
      */
    def onResolverClock: At[Clock.Resolver] = t

type EdgeTime = At[Clock.Edge]
type LogTime = At[Clock.Log]
type ResolverTime = At[Clock.Resolver]
type WallTime = At[Clock.Wall]
type HistoryTime = At[Clock.History]

object EdgeTime:
  def apply(millis: Long): EdgeTime = At(millis)

object LogTime:
  def apply(millis: Long): LogTime = At(millis)

object ResolverTime:
  def apply(millis: Long): ResolverTime = At(millis)

object WallTime:
  def apply(millis: Long): WallTime = At(millis)

object HistoryTime:
  def apply(millis: Long): HistoryTime = At(millis)

/** One step of a machine: the new state, the effects the shell must perform, and the coverage marks hit. */
final case class Step[+S, +F](state: S, effects: Vector[F] = Vector.empty, marks: Vector[Mark] = Vector.empty):
  def flatMap[S2, F2 >: F](f: S => Step[S2, F2]): Step[S2, F2] =
    val next = f(state)
    Step(next.state, effects ++ next.effects, marks ++ next.marks)

  def mark(m: Mark): Step[S, F] = copy(marks = marks :+ m)

/** A process as the interpreters run it: the state it starts with, holding nothing, and its step (README section 11,
  * processes). `T` is what the process reads from its clocks at each step. Every process has this one shape, so the
  * simulator runs them alike: a crash drops the state and restarts from `initial`, a pause holds back the events,
  * and a planted bug rewrites the effects.
  */
final case class Machine[S, E, F, T](initial: S, step: (S, T, E) => Step[S, F])
