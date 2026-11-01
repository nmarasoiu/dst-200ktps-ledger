package ledger.sim

import ledger.core.*
import scala.concurrent.duration.FiniteDuration

/** One process's clock of kind `C`: the history's time plus a skew the seed chose. Safety must hold under any skew;
  * only timeliness may suffer (README section 11, simulator).
  */
final case class SkewedClock[C <: Clock](skewMillis: Long):
  def read(t: HistoryTime): At[C] = At[C](t.millis + skewMillis)

  /** The history's time when this clock reaches a reading: when to deliver a wake-up the process asked for. */
  def toHistory(at: At[C]): HistoryTime = HistoryTime(at.millis - skewMillis)

object SkewedClock:
  def chosen[C <: Clock](rng: Rng, maxSkew: FiniteDuration): SkewedClock[C] =
    SkewedClock(rng.between(-maxSkew.toMillis, maxSkew.toMillis))
