package ledger.shell

import cats.effect.IO
import cats.syntax.all.*
import io.circe.syntax.*
import io.circe.{Decoder, DecodingFailure, HCursor, Json}
import ledger.core.*
import org.http4s.circe.*
import org.http4s.dsl.io.*
import org.http4s.{HttpRoutes, Method, Request as HttpRequest, Response, Status, Uri}

/** The caller API (README section 4): JSON over HTTP. A number too wide for JSON's own, an amount or an account, goes as
  * a decimal string. The HTTP status says only what a caller must do next; the body says why.
  */
object EdgeApi:
  private given CanEqual[Method, Method] = CanEqual.derived
  private given CanEqual[Uri.Path, Uri.Path] = CanEqual.derived

  private def fail[A](what: String, c: HCursor): Decoder.Result[A] = Left(DecodingFailure(what, c.history))

  private def wide(c: HCursor, field: String): Decoder.Result[U128] =
    c.get[String](field).flatMap: text =>
      Either.catchNonFatal(U128.fromBigInt(BigInt(text))).leftFlatMap(e => fail(s"$field: ${e.getMessage}", c))

  private def keyOf(c: HCursor): Decoder.Result[ClientKey] =
    for
      createdAt <- c.get[Long]("createdAt")
      body <- c.get[String]("key")
      key <- ClientKey.from(WallTime(createdAt), body).fold(fail(_, c), Right(_))
    yield key

  private def amountOf(c: HCursor): Decoder.Result[CaptureAmount] =
    c.get[String]("amount").flatMap:
      case "full" => Right(CaptureAmount.Full)
      case text => text.toLongOption.toRight(s"not an amount: $text").flatMap(CaptureAmount.Partial.from).fold(fail(_, c), Right(_))

  def authorize(json: Json): Decoder.Result[Request] =
    val c = json.hcursor
    for
      key <- keyOf(c)
      debit <- wide(c, "debit")
      credit <- wide(c, "credit")
      amount <- wide(c, "amount")
      ledger <- c.get[Int]("ledger")
    yield Request.Authorize(key, AccountId(debit), AccountId(credit), amount, ledger)

  def cancel(json: Json): Decoder.Result[Request] = keyOf(json.hcursor).map(Request.Cancel(_))

  def capture(json: Json): Decoder.Result[Request] =
    val c = json.hcursor
    (keyOf(c), amountOf(c)).mapN(Request.Capture(_, _))

  /** What a caller sends, as the workload driver and any other client build it. */
  def encode(request: Request): (String, Json) =
    def keyed(key: ClientKey) = Vector("createdAt" -> key.createdAt.millis.asJson, "key" -> key.body.asJson)
    request match
      case Request.Authorize(key, debit, credit, amount, ledger) =>
        val fields = Vector(
          "debit" -> debit.value.toBigInt.toString.asJson,
          "credit" -> credit.value.toBigInt.toString.asJson,
          "amount" -> amount.toBigInt.toString.asJson,
          "ledger" -> ledger.asJson,
        )
        ("authorize", Json.fromFields(keyed(key) ++ fields))
      case Request.Cancel(key) => ("cancel", Json.fromFields(keyed(key)))
      case Request.Capture(key, amount) => ("capture", Json.fromFields(keyed(key) :+ ("amount" -> amountText(amount).asJson)))

  private def amountText(amount: CaptureAmount): String = amount match
    case CaptureAmount.Full => "full"
    case CaptureAmount.Partial(minorUnits) => minorUnits.toString

  private def declineText(r: DeclineReason): String = r match
    case DeclineReason.Unknown => "unknown"
    case DeclineReason.NotExecuted => "not_executed"
    case DeclineReason.KeyReused(status) => s"key_reused:$status"
    case DeclineReason.Ledger(status) => s"ledger:$status"

  private def refusalText(r: CaptureRefusal): String = r match
    case CaptureRefusal.NotApproved => "not_approved"
    case CaptureRefusal.Cancelled => "cancelled"
    case CaptureRefusal.Expired => "expired"
    case CaptureRefusal.ExceedsHold => "exceeds_hold"
    case CaptureRefusal.KeyReused(status) => s"key_reused:$status"
    case CaptureRefusal.Ledger(status) => s"ledger:$status"

  private def result(name: String, fields: (String, Json)*): Json = Json.fromFields(("result" -> name.asJson) +: fields)

  def encode(reply: Reply): Json = reply match
    case Reply.Decided(AuthOutcome.Approved) => result("approved")
    case Reply.Decided(AuthOutcome.Declined(reason)) => result("declined", "reason" -> declineText(reason).asJson)
    case Reply.Decided(CancelOutcome.Cancelled) => result("cancelled")
    case Reply.Decided(CancelOutcome.CancelRefused) => result("cancel_refused")
    case Reply.Decided(CaptureOutcome.Captured(amount)) => result("captured", "amount" -> amountText(amount).asJson)
    case Reply.Decided(CaptureOutcome.CaptureRefused(reason)) => result("capture_refused", "reason" -> refusalText(reason).asJson)
    case Reply.Refused(reason) => result("refused", "reason" -> reason.toString.asJson)
    case Reply.TimedOut => result("timed_out")
    case Reply.Retry => result("retry")

  private def status(text: String, tail: String): Either[String, TbStatus] =
    Either.catchNonFatal(TbStatus.valueOf(tail)).leftMap(_ => s"unknown status in $text")

  private def declineFrom(text: String): Either[String, DeclineReason] = text.split(":", 2).toList match
    case List("unknown") => Right(DeclineReason.Unknown)
    case List("not_executed") => Right(DeclineReason.NotExecuted)
    case List("key_reused", s) => status(text, s).map(DeclineReason.KeyReused(_))
    case List("ledger", s) => status(text, s).map(DeclineReason.Ledger(_))
    case _ => Left(s"unknown decline reason $text")

  private def refusalFrom(text: String): Either[String, CaptureRefusal] = text.split(":", 2).toList match
    case List("not_approved") => Right(CaptureRefusal.NotApproved)
    case List("cancelled") => Right(CaptureRefusal.Cancelled)
    case List("expired") => Right(CaptureRefusal.Expired)
    case List("exceeds_hold") => Right(CaptureRefusal.ExceedsHold)
    case List("key_reused", s) => status(text, s).map(CaptureRefusal.KeyReused(_))
    case List("ledger", s) => status(text, s).map(CaptureRefusal.Ledger(_))
    case _ => Left(s"unknown capture refusal $text")

  /** What a client hears, read back, as the workload driver records it (README section 11, one history). */
  def replyFrom(json: Json): Either[String, Reply] =
    val c = json.hcursor
    def field(name: String) = c.get[String](name).leftMap(_.getMessage)
    field("result").flatMap:
      case "approved" => Right(Reply.Decided(AuthOutcome.Approved))
      case "declined" => field("reason").flatMap(declineFrom).map(r => Reply.Decided(AuthOutcome.Declined(r)))
      case "cancelled" => Right(Reply.Decided(CancelOutcome.Cancelled))
      case "cancel_refused" => Right(Reply.Decided(CancelOutcome.CancelRefused))
      case "captured" => amountOf(c).leftMap(_.getMessage).map(a => Reply.Decided(CaptureOutcome.Captured(a)))
      case "capture_refused" => field("reason").flatMap(refusalFrom).map(r => Reply.Decided(CaptureOutcome.CaptureRefused(r)))
      case "refused" =>
        field("reason").flatMap(r => Either.catchNonFatal(RefusalReason.valueOf(r)).leftMap(_ => s"unknown refusal $r")).map(Reply.Refused(_))
      case "timed_out" => Right(Reply.TimedOut)
      case "retry" => Right(Reply.Retry)
      case other => Left(s"unknown result $other")

  /** 200 for a decision. 503 and 504 for what is not one: resend a refused authorization or a retry, and cancel after a
    * timeout. A caller that reads only the status still does the right thing with 504 and 200.
    */
  private def statusOf(reply: Reply): Status = reply match
    case _: Reply.Decided => Status.Ok
    case _: Reply.Refused | Reply.Retry => Status.ServiceUnavailable
    case Reply.TimedOut => Status.GatewayTimeout

  def routes(edge: EdgeProcess): HttpRoutes[IO] =
    def answer(req: HttpRequest[IO], decode: Json => Decoder.Result[Request]): IO[Response[IO]] =
      req.as[Json].attempt.map(_.leftMap(_.getMessage).flatMap(decode(_).leftMap(_.getMessage))).flatMap:
        case Left(problem) => BadRequest(result("malformed", "reason" -> problem.asJson))
        case Right(request) => edge.ask(request).map(reply => Response[IO](statusOf(reply)).withEntity(encode(reply)))
    HttpRoutes.of[IO]:
      case req @ POST -> Root / "authorize" => answer(req, authorize)
      case req @ POST -> Root / "cancel" => answer(req, cancel)
      case req @ POST -> Root / "capture" => answer(req, capture)
      case GET -> Root / "health" => Ok()
