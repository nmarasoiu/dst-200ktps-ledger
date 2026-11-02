package ledger.core

import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

class RecordCodecSuite extends ScalaCheckSuite:

  private val u128: Gen[U128] = for hi <- Gen.long; lo <- Gen.long yield U128(hi, lo)

  private val command: Gen[Command] = Gens.key.flatMap: key =>
    Gen.oneOf(
      for debit <- u128; credit <- u128; amount <- u128; ledger <- Gen.posNum[Int]; hold <- Gen.posNum[Int]
      yield Command.Authorize(key, AccountId(debit), AccountId(credit), amount, ledger, hold),
      Gen.const(Command.Cancel(key)),
      Gens.captureAmount.map(Command.Capture(key, _)),
    )

  private val record: Gen[Record] = Gen.oneOf(
    command.map(Record.CommandRecord(_)),
    for c <- command; o <- Gens.outcome; w <- Gen.oneOf(Writer.values.toSeq) yield Record.OutcomeRecord(c, o, w),
  )

  property("a record reads back as it was written"):
    forAll(record)(r => RecordCodec.decode(RecordCodec.encode(r)) == Right(r))

  property("every prefix of a record is an error, never a different record"):
    forAll(record): r =>
      val bytes = RecordCodec.encode(r)
      (0 until bytes.length).forall(n => RecordCodec.decode(bytes.take(n)).isLeft)

  test("an unknown version, tag or status is an error, since the resolver must not read past it"):
    val bytes = RecordCodec.encode(Record.CommandRecord(Command.Cancel(ClientKey(WallTime(5), "k"))))
    assert(RecordCodec.decode(bytes.updated(0, 9.toByte)).left.exists(_.contains("version")))
    assert(RecordCodec.decode(bytes.updated(1, 9.toByte)).isLeft)
    assert(RecordCodec.decode(bytes :+ 0.toByte).left.exists(_.contains("after the record")))

  // The log keeps records for 30 days across deployments, so a format change must be deliberate.
  test("the format does not change by accident"):
    val r = Record.OutcomeRecord(Command.Capture(ClientKey(WallTime(1), "k"), CaptureAmount.Partial(2)), CaptureOutcome.CaptureRefused(CaptureRefusal.Ledger(TbStatus.Exists)), Writer.Resolver)
    val hex = RecordCodec.encode(r).map(b => f"$b%02x").mkString
    assertEquals(hex, "01010200000000000000010001" + "6b" + "010000000000000002" + "0505" + "0006" + "457869737473" + "01")
