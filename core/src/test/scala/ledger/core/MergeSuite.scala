package ledger.core

import AuthOutcome.{Approved, Declined}
import CancelOutcome.{CancelRefused, Cancelled}
import CaptureOutcome.{CaptureRefused, Captured}
import Merge.Ending
import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

class MergeSuite extends ScalaCheckSuite:

  private val holds = Vector(1, 2, 3).map(n => Ids.authorization(Fixtures.key(n)))
  private val entries: Gen[Vector[(TransferId, Outcome)]] =
    Gen.containerOf[Vector, (TransferId, Outcome)](Gen.zip(Gen.oneOf(holds), Gens.outcome))

  property("the merge does not depend on the order outcomes arrive in"):
    forAll(entries, Gen.long): (xs, seed) =>
      val shuffled = new scala.util.Random(seed).shuffle(xs)
      Merge.fold(shuffled) == Merge.fold(xs)

  property("merging a duplicate changes nothing"):
    forAll(entries, Gen.choose(0, 5)): (xs, n) =>
      Merge.fold(xs ++ xs.take(n)) == Merge.fold(xs)

  property("decisions and conflicts depend only on the set of outcomes"):
    forAll(entries, Gen.long): (xs, seed) =>
      val a = Merge.fold(xs)
      val b = Merge.fold(new scala.util.Random(seed).shuffle(xs ++ xs))
      a.keySet == b.keySet && a.keys.forall(k => a(k).ending == b(k).ending && a(k).conflicts == b(k).conflicts)

  private val a = holds.head
  private def merged(outcomes: Outcome*): Merge.Hold = Merge.fold(outcomes.map(a -> _))(a)
  private val half = CaptureAmount.Partial(50)

  // Every pair the tables in README sections 5 and 8 can produce for one command or one hold.

  test("the edge's approval and the resolver's fence finding it: approved"):
    assertEquals(merged(Approved, Approved).authorizationDecision, Right(Some(Approved)))

  test("the edge's reason and the resolver's generic one: the edge's reason is kept"):
    val specific = Declined(DeclineReason.Ledger(TbStatus.ExceedsCredits))
    assertEquals(merged(specific, Declined(DeclineReason.Unknown)).authorizationDecision, Right(Some(specific)))

  test("a declined authorization, then a duplicate after a cancel's fence: still declined"):
    val rejected = Declined(DeclineReason.Ledger(TbStatus.AccountsMustBeDifferent))
    assertEquals(merged(rejected, Declined(DeclineReason.NotExecuted)).authorizationDecision, Right(Some(rejected)))

  test("not_executed from expiry and from a cancel's fence are one outcome"):
    assertEquals(merged(Declined(DeclineReason.NotExecuted), Declined(DeclineReason.NotExecuted)).conflicts, Vector.empty)

  test("a copy declined for reusing the key with a different payload sits beside the approval"):
    val reused = Declined(DeclineReason.KeyReused(TbStatus.ExistsWithDifferentAmount))
    assertEquals(merged(Approved, reused).authorizationDecision, Right(Some(Approved)))
    assertEquals(merged(reused).authorizationDecision, Right(Some(reused)))

  test("approved and declined for one authorization is a conflict"):
    assert(merged(Approved, Declined(DeclineReason.NotExecuted)).conflicts.nonEmpty)

  test("a cancel keeps the hold cancelled even when a duplicate approval lands after it"):
    assertEquals(merged(Approved, Cancelled, Approved).ending, Right(Ending.Cancelled))
    assertEquals(merged(Cancelled, Approved).ending, Right(Ending.Cancelled))

  test("the caller's cancel and the edge's own cancel: one outcome"):
    assertEquals(merged(Cancelled, Cancelled).cancelDecision, Right(Some(Cancelled)))

  test("a cancel that found the capture, and the capture: captured"):
    assertEquals(merged(Approved, CancelRefused, Captured(half)).ending, Right(Ending.Captured(Some(half))))

  test("a capture and its resend: captured once"):
    assertEquals(merged(Captured(half), Captured(half)).captureDecision, Right(Some(Captured(half))))

  test("a capture refused as exceeding the hold, then a corrected one: captured"):
    val corrected = Captured(CaptureAmount.Partial(10))
    assertEquals(merged(CaptureRefused(CaptureRefusal.ExceedsHold), corrected).captureDecision, Right(Some(corrected)))

  test("cancelled and captured is two endings"):
    assert(merged(Approved, Cancelled, Captured(half)).conflicts.nonEmpty)

  test("cancelled and cancel_refused is two decisions"):
    assert(merged(Cancelled, CancelRefused).conflicts.nonEmpty)

  test("a declined authorization whose hold was captured is two endings"):
    assert(merged(Declined(DeclineReason.NotExecuted), Captured(half)).conflicts.nonEmpty)

  test("captured, then refused as cancelled, is a conflict"):
    assert(merged(Captured(half), CaptureRefused(CaptureRefusal.Cancelled)).conflicts.nonEmpty)

  test("a full capture beside a partial of the whole hold is one capture, and the partial says how much"):
    val whole = CaptureAmount.Partial(100)
    val hold = Merge.fold(Vector(a -> Captured(CaptureAmount.Full), a -> Captured(whole)))(a)
    assertEquals(hold.captureDecision, Right(Some(Captured(whole))))
    assertEquals(hold.conflicts, Vector.empty)

  test("two different partial captures still conflict"):
    val hold = Merge.fold(Vector(a -> Captured(CaptureAmount.Partial(10)), a -> Captured(CaptureAmount.Partial(20))))(a)
    assert(hold.captureDecision.isLeft)

  test("an approved hold with nothing after it is open"):
    assertEquals(merged(Approved).ending, Right(Ending.Open))
