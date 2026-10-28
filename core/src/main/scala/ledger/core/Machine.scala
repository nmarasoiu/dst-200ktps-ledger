package ledger.core

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

/** A time in milliseconds, read from clock `C`. An opaque `Long`: stored as a primitive, while times from different
  * clocks stay different types.
  */
opaque type At[C <: Clock] = Long

object At:
  def apply[C <: Clock](millis: Long): At[C] = millis

  given [C <: Clock] => Ordering[At[C]] = Ordering.Long

  extension [C <: Clock](t: At[C])
    def millis: Long = t
    def +(d: FiniteDuration): At[C] = t + d.toMillis
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

object EdgeTime:
  def apply(millis: Long): EdgeTime = At(millis)

object LogTime:
  def apply(millis: Long): LogTime = At(millis)

object ResolverTime:
  def apply(millis: Long): ResolverTime = At(millis)

/** One step of a machine: the new state, the effects the shell must perform, and the coverage marks hit. */
final case class Step[+S, +F](state: S, effects: Vector[F] = Vector.empty, marks: Vector[Mark] = Vector.empty):
  def flatMap[S2, F2 >: F](f: S => Step[S2, F2]): Step[S2, F2] =
    val next = f(state)
    Step(next.state, effects ++ next.effects, marks ++ next.marks)

  def mark(m: Mark): Step[S, F] = copy(marks = marks :+ m)
