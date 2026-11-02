package ledger.shell

import cats.effect.IO
import io.circe.Json
import io.circe.jawn.parse
import ledger.core.*
import cats.syntax.all.*
import munit.CatsEffectSuite
import org.http4s.circe.*
import org.http4s.{Method, Request as HttpRequest, Status}
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

class EdgeApiSuite extends CatsEffectSuite with munit.ScalaCheckSuite:

  private val reply: Gen[Reply] = Gen.oneOf(
    Gens.outcome.map(Reply.Decided(_)),
    Gen.oneOf(RefusalReason.values.toSeq).map(Reply.Refused(_)),
    Gen.const(Reply.TimedOut),
    Gen.const(Reply.Retry),
  )

  private val request: Gen[Request] = Gens.key.flatMap: key =>
    Gen.oneOf(
      for debit <- Gen.posNum[Long]; credit <- Gen.posNum[Long]; amount <- Gen.posNum[Long]; ledger <- Gen.posNum[Int]
      yield Request.Authorize(key, AccountId.of(debit), AccountId.of(credit), U128.of(amount), ledger),
      Gen.const(Request.Cancel(key)),
      Gens.captureAmount.map(Request.Capture(key, _)),
    )

  property("a reply reads back as it was sent"):
    forAll(reply)(r => EdgeApi.replyFrom(EdgeApi.encode(r)) == Right(r))

  property("a request reads back as it was sent"):
    forAll(request): r =>
      val (path, json) = EdgeApi.encode(r)
      val read = path match
        case "authorize" => EdgeApi.authorize(json)
        case "cancel" => EdgeApi.cancel(json)
        case _ => EdgeApi.capture(json)
      read == Right(r)

  test("an account as wide as TigerBeetle allows survives the trip"):
    val r = Request.Authorize(Simulated.key(1), AccountId(U128.Max), AccountId.of(1), U128.Max, 7)
    assertEquals(EdgeApi.authorize(EdgeApi.encode(r)._2), Right(r))

  private def answering(reply: Reply): org.http4s.HttpApp[IO] =
    EdgeApi.routes((_: Request) => IO.pure(reply)).orNotFound

  private def post(path: String, body: String) =
    val parsed = parse(body).getOrElse(Json.Null)
    HttpRequest[IO](Method.POST, org.http4s.Uri.unsafeFromString(s"/$path")).withEntity(parsed)

  private val cancel = """{"createdAt": 1700000000000, "key": "k"}"""

  test("the status says what a caller must do next, and the body says why"):
    val cases = Vector(
      Reply.Decided(CancelOutcome.Cancelled) -> Status.Ok,
      Reply.Refused(RefusalReason.Overload) -> Status.ServiceUnavailable,
      Reply.Retry -> Status.ServiceUnavailable,
      Reply.TimedOut -> Status.GatewayTimeout,
    )
    cases.traverse_ : (r, status) =>
      answering(r).run(post("cancel", cancel)).flatMap: response =>
        assertEquals(response.status, status)
        response.as[Json].map(json => assertEquals(EdgeApi.replyFrom(json), Right(r)))

  test("a malformed request is a 400, and never reaches the machine"):
    val bad = Vector(
      "authorize" -> """{"createdAt": 1700000000000, "key": "k"}""",
      "authorize" -> """{"createdAt": 1700000000000, "key": "k", "debit": "-1", "credit": "2", "amount": "3", "ledger": 7}""",
      "cancel" -> """{"createdAt": -5, "key": "k"}""",
      "capture" -> """{"createdAt": 1700000000000, "key": "k", "amount": "0"}""",
      "capture" -> """{"createdAt": 1700000000000, "key": "k", "amount": "nine"}""",
    )
    val app = EdgeApi.routes((_: Request) => IO.raiseError(AssertionError("reached the machine"))).orNotFound
    bad.traverse_((path, body) => app.run(post(path, body)).map(r => assertEquals(r.status, Status.BadRequest, s"$path $body")))
