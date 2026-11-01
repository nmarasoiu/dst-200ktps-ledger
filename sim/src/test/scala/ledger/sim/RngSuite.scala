package ledger.sim

import java.lang.Long.toUnsignedString
import munit.ScalaCheckSuite
import org.scalacheck.Prop.forAll

class RngSuite extends ScalaCheckSuite:

  // A seed must mean the same for as long as a failing seed is worth replaying.
  test("the output matches SplitMix64's published test vector for seed 1234567"):
    val rng = new Rng(1234567L)
    assertEquals(
      Vector.fill(5)(toUnsignedString(rng.nextLong())),
      Vector("6457827717110365317", "3203168211198807973", "9817491932198370423", "4593380528125082431", "16408922859458223821"),
    )

  property("a seed gives the same numbers every time"):
    forAll: (seed: Long) =>
      val (a, b) = (new Rng(seed), new Rng(seed))
      Vector.fill(20)(a.nextLong()) == Vector.fill(20)(b.nextLong())

  property("between stays within its bounds"):
    forAll: (seed: Long, lo: Short, span: Short) =>
      val hi = lo.toLong + span.toLong.abs
      val rng = new Rng(seed)
      Vector.fill(50)(rng.between(lo.toLong, hi)).forall(n => n >= lo && n <= hi)

  test("a chance of 0 is never true, and a chance of 1 is always true"):
    val rng = new Rng(7L)
    assert(Vector.fill(1000)(rng.chance(0.0)).forall(!_))
    assert(Vector.fill(1000)(rng.chance(1.0)).forall(identity))

  test("a chance of a half comes up about half the time, and pick reaches every element"):
    val rng = new Rng(7L)
    val heads = Vector.fill(10000)(rng.chance(0.5)).count(identity)
    assert(heads > 4500 && heads < 5500, s"$heads heads")
    assertEquals(Vector.fill(100)(rng.pick(Vector(1, 2, 3))).toSet, Set(1, 2, 3))
