package ledger.sim

import java.lang.Long.remainderUnsigned

/** The seeded generator behind every choice of a run: SplitMix64, a fixed algorithm kept here rather than a JDK
  * class, so a seed means the same on any JDK (README section 11, simulator). `RngSuite` pins its output.
  */
final class Rng(seed: Long):
  private var state = seed

  def nextLong(): Long =
    state += 0x9e3779b97f4a7c15L
    var z = state
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL
    z ^ (z >>> 31)

  /** A number in `[lo, hi]`. */
  def between(lo: Long, hi: Long): Long =
    require(lo <= hi, s"empty range: [$lo, $hi]")
    lo + remainderUnsigned(nextLong(), hi - lo + 1)

  /** True with this probability. */
  def chance(probability: Double): Boolean = (nextLong() >>> 11).toDouble / (1L << 53).toDouble < probability

  def pick[A](xs: IndexedSeq[A]): A = xs(between(0, xs.size - 1).toInt)
