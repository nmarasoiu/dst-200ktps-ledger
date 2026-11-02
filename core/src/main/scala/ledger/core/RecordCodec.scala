package ledger.core

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream, IOException}
import scala.util.control.NonFatal

/** A record of the command log as bytes: the edge and the resolver write it, and the resolver and the offline checker
  * read it for as long as the log keeps it (README section 4). Fields are written in a fixed order after a version
  * byte. A TigerBeetle status goes by name, since its numeric code is TigerBeetle's and its position here is not
  * stable.
  */
object RecordCodec:
  private val Version = 1

  def encode(record: Record): Array[Byte] =
    val bytes = new ByteArrayOutputStream(96)
    val out = new DataOutputStream(bytes)
    out.writeByte(Version)
    record match
      case Record.CommandRecord(command) =>
        out.writeByte(0)
        writeCommand(out, command)
      case Record.OutcomeRecord(command, outcome, writer) =>
        out.writeByte(1)
        writeCommand(out, command)
        writeOutcome(out, outcome)
        out.writeByte(writer.ordinal)
    bytes.toByteArray

  /** A record it cannot read is an error, never something to skip: the resolver must not pass a command unseen. */
  def decode(bytes: Array[Byte]): Either[String, Record] =
    val in = new DataInputStream(new ByteArrayInputStream(bytes))
    try
      val version = in.readByte().toInt
      if version != Version then Left(s"unknown record version $version")
      else
        val record = in.readByte() match
          case 0 => Record.CommandRecord(readCommand(in))
          case 1 =>
            val command = readCommand(in)
            val outcome = readOutcome(in)
            Record.OutcomeRecord(command, outcome, Writer.fromOrdinal(in.readByte().toInt))
          case tag => unknown("record", tag)
        if in.available() > 0 then Left(s"${in.available()} bytes after the record") else Right(record)
    catch case NonFatal(e) => Left(s"unreadable record: $e")

  private def unknown(what: String, tag: Int): Nothing = throw IOException(s"unknown $what tag $tag")

  private def writeU128(out: DataOutputStream, n: U128): Unit =
    out.writeLong(n.hi)
    out.writeLong(n.lo)

  private def readU128(in: DataInputStream): U128 =
    val hi = in.readLong()
    U128(hi, in.readLong())

  private def writeKey(out: DataOutputStream, key: ClientKey): Unit =
    out.writeLong(key.createdAt.millis)
    out.writeUTF(key.body)

  private def readKey(in: DataInputStream): ClientKey =
    val createdAt = WallTime(in.readLong())
    ClientKey.from(createdAt, in.readUTF()).fold(e => throw IOException(e), identity)

  private def writeAmount(out: DataOutputStream, amount: CaptureAmount): Unit = amount match
    case CaptureAmount.Full => out.writeByte(0)
    case CaptureAmount.Partial(minorUnits) =>
      out.writeByte(1)
      out.writeLong(minorUnits)

  private def readAmount(in: DataInputStream): CaptureAmount = in.readByte() match
    case 0 => CaptureAmount.Full
    case 1 => CaptureAmount.Partial.from(in.readLong()).fold(e => throw IOException(e), identity)
    case tag => unknown("capture amount", tag)

  private def writeCommand(out: DataOutputStream, command: Command): Unit = command match
    case Command.Authorize(key, debit, credit, amount, ledger, holdTimeoutSeconds) =>
      out.writeByte(0)
      writeKey(out, key)
      writeU128(out, debit.value)
      writeU128(out, credit.value)
      writeU128(out, amount)
      out.writeInt(ledger)
      out.writeInt(holdTimeoutSeconds)
    case Command.Cancel(key) =>
      out.writeByte(1)
      writeKey(out, key)
    case Command.Capture(key, amount) =>
      out.writeByte(2)
      writeKey(out, key)
      writeAmount(out, amount)

  private def readCommand(in: DataInputStream): Command = in.readByte() match
    case 0 =>
      val key = readKey(in)
      val debit = AccountId(readU128(in))
      val credit = AccountId(readU128(in))
      val amount = readU128(in)
      val ledger = in.readInt()
      Command.Authorize(key, debit, credit, amount, ledger, in.readInt())
    case 1 => Command.Cancel(readKey(in))
    case 2 =>
      val key = readKey(in)
      Command.Capture(key, readAmount(in))
    case tag => unknown("command", tag)

  private def writeStatus(out: DataOutputStream, status: TbStatus): Unit = out.writeUTF(status.toString)
  private def readStatus(in: DataInputStream): TbStatus = TbStatus.valueOf(in.readUTF())

  private def writeOutcome(out: DataOutputStream, outcome: Outcome): Unit = outcome match
    case AuthOutcome.Approved => out.writeByte(0)
    case AuthOutcome.Declined(reason) =>
      out.writeByte(1)
      reason match
        case DeclineReason.Unknown => out.writeByte(0)
        case DeclineReason.NotExecuted => out.writeByte(1)
        case DeclineReason.KeyReused(status) =>
          out.writeByte(2)
          writeStatus(out, status)
        case DeclineReason.Ledger(status) =>
          out.writeByte(3)
          writeStatus(out, status)
    case CancelOutcome.Cancelled => out.writeByte(2)
    case CancelOutcome.CancelRefused => out.writeByte(3)
    case CaptureOutcome.Captured(amount) =>
      out.writeByte(4)
      writeAmount(out, amount)
    case CaptureOutcome.CaptureRefused(reason) =>
      out.writeByte(5)
      reason match
        case CaptureRefusal.NotApproved => out.writeByte(0)
        case CaptureRefusal.Cancelled => out.writeByte(1)
        case CaptureRefusal.Expired => out.writeByte(2)
        case CaptureRefusal.ExceedsHold => out.writeByte(3)
        case CaptureRefusal.KeyReused(status) =>
          out.writeByte(4)
          writeStatus(out, status)
        case CaptureRefusal.Ledger(status) =>
          out.writeByte(5)
          writeStatus(out, status)

  private def readOutcome(in: DataInputStream): Outcome = in.readByte() match
    case 0 => AuthOutcome.Approved
    case 1 =>
      AuthOutcome.Declined(in.readByte() match
        case 0 => DeclineReason.Unknown
        case 1 => DeclineReason.NotExecuted
        case 2 => DeclineReason.KeyReused(readStatus(in))
        case 3 => DeclineReason.Ledger(readStatus(in))
        case tag => unknown("decline reason", tag))
    case 2 => CancelOutcome.Cancelled
    case 3 => CancelOutcome.CancelRefused
    case 4 => CaptureOutcome.Captured(readAmount(in))
    case 5 =>
      CaptureOutcome.CaptureRefused(in.readByte() match
        case 0 => CaptureRefusal.NotApproved
        case 1 => CaptureRefusal.Cancelled
        case 2 => CaptureRefusal.Expired
        case 3 => CaptureRefusal.ExceedsHold
        case 4 => CaptureRefusal.KeyReused(readStatus(in))
        case 5 => CaptureRefusal.Ledger(readStatus(in))
        case tag => unknown("capture refusal", tag))
    case tag => unknown("outcome", tag)
