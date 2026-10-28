package ledger.core

import Caller.{Effect, Event, Phase}
import Fixtures.*
import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

class CallerSuite extends ScalaCheckSuite:

  private val k = key(1)
  private val auth = authorize(k)

  private def run(events: Seq[Event]): Vector[Step[Caller.State, Effect]] =
    events.foldLeft(Vector(Caller.start(auth)))((steps, e) => steps :+ Caller.step(steps.last.state, e))

  private def sent(steps: Vector[Step[Caller.State, Effect]]): Vector[Request] = steps.flatMap(_.effects).map { case Effect.Send(r) => r }

  test("section 8: a timed-out authorization is cancelled, and the cancel resent until it is answered"):
    val steps = run(Seq(Event.Replied(Reply.TimedOut), Event.Lost, Event.Replied(Reply.Retry), Event.Replied(Reply.Decided(CancelOutcome.Cancelled))))
    assertEquals(sent(steps), Vector(auth, Request.Cancel(k), Request.Cancel(k), Request.Cancel(k)))
    assertEquals(steps.last.state.phase, Phase.Done)

  test("section 5: a lost reply is a timeout from the caller's view, so the caller cancels"):
    assertEquals(sent(run(Seq(Event.Lost))), Vector(auth, Request.Cancel(k)))

  test("section 5: a refusal may be resent with the same key, and needs no cancel"):
    val steps = run(Seq(Event.Replied(Reply.Refused(RefusalReason.Overload)), Event.Resend, Event.Replied(Reply.Decided(AuthOutcome.Approved))))
    assertEquals(sent(steps), Vector(auth, auth))
    assertEquals(steps.last.state.phase, Phase.Approved)

  test("section 8: a capture follows an approval, and is resent with the same key until answered"):
    val half = CaptureAmount.Partial(50)
    val steps = run(Seq(Event.Replied(Reply.Decided(AuthOutcome.Approved)), Event.Capture(half), Event.Replied(Reply.Retry), Event.Lost))
    assertEquals(sent(steps), Vector(auth, Request.Capture(k, half), Request.Capture(k, half), Request.Capture(k, half)))

  test("section 5: a capture can only follow an approval"):
    val steps = run(Seq(Event.Capture(CaptureAmount.Full)))
    assertEquals(sent(steps), Vector(auth))
    assertEquals(steps.last.marks, Vector(Mark.CallerIgnoredOutOfTurn))

  private val events: Gen[Vector[Event]] = Gen
    .listOf(Gen.oneOf(
      Gen.const(Event.Lost),
      Gen.const(Event.Resend),
      Gen.const(Event.Stop),
      Gens.captureAmount.map(Event.Capture(_)),
      Gen.oneOf(Reply.TimedOut, Reply.Retry, Reply.Refused(RefusalReason.Overload)).map(Event.Replied(_)),
      Gens.outcome.map(o => Event.Replied(Reply.Decided(o))),
    ))
    .map(_.toVector)

  property("the caller never resends an authorization after a non-definitive result"):
    forAll(events): es =>
      val steps = run(es)
      val nonDefinitiveAt = steps.indexWhere(_.marks.contains(Mark.CallerCancelsAfterNonDefinitive))
      nonDefinitiveAt < 0 || !steps.drop(nonDefinitiveAt).flatMap(_.effects).contains(Effect.Send(auth))

  property("the caller cancels only after a non-definitive result, and captures only after an approval"):
    forAll(events): es =>
      val steps = run(es)
      val phases = steps.map(_.state.phase)
      steps.indices.forall: i =>
        steps(i).effects.forall:
          case Effect.Send(_: Request.Cancel) => phases.take(i + 1).contains(Phase.Cancelling)
          case Effect.Send(_: Request.Capture) => phases.take(i).contains(Phase.Approved)
          case _ => true
