package ledger.shell

import cats.effect.IO
import fs2.Stream
import ledger.core.*
import ledger.sim.{SimKafka, SimTigerBeetle}
import scala.concurrent.duration.*

/** The ports on the simulated adapters, which the contract suites hold to the real systems (README section 11, ports).
  * Under TestControl only one fiber runs at a time, so these need no locks. Times come from `IO`, which TestControl
  * moves.
  */
object Simulated:
  val Partitions = 4
  val Funded: AccountId = AccountId.of(1000)
  val Merchant: AccountId = AccountId.of(3000)
  private val Source = AccountId.of(4000)
  private val AccountLedger = 7
  val ledgerConfig: LedgerConfig = LedgerConfig(AccountId.of(9001), AccountId.of(9002), 99, 90, 71)

  val noAlarm: Alarm => IO[Unit] = _ => IO.unit

  def request(n: Int): EdgeRequest.Config =
    EdgeRequest.Config(1500.millis, 1.second, 1.second, n, PeriodSchedule.Single, ledgerConfig)

  def edge(maxInFlight: Int = 64): Edge.Config =
    Edge.Config(request(3600), Admission.Config(maxInFlight, 300.millis, Partitions))

  val resolver: ResolverPartition.Config =
    ResolverPartition.Config(1500.millis, 3.seconds, 300.millis, PeriodSchedule.Single, ledgerConfig)

  def key(n: Int): ClientKey = ClientKey(WallTime(1_700_000_000_000L + n), s"key-$n")
  def authorize(n: Int): Request.Authorize = Request.Authorize(key(n), Funded, Merchant, U128.of(10), AccountLedger)

  def command(n: Int): Command = Command.stamped(authorize(n), 3600)

  /** Acknowledges each write after `ack`. */
  final class Log(val kafka: SimKafka = new SimKafka(Partitions, 2000L), ack: FiniteDuration = 5.millis) extends LogWriter:
    def append(append: CommandLog.Append): IO[CommandLog.Written] =
      IO.sleep(ack) >> IO.realTime.map(t => kafka.append(append.record, LogTime(t.toMillis)))
    def leaders: IO[CommandLog.Leaders] = IO(kafka.leaders)
    def records: Vector[Record] = (0 until Partitions).toVector.flatMap(i => kafka.records(Partition(i)).map(_.record))

  /** Answers each call after `after(call)`. */
  final class Tb(after: Ledger.Submit => FiniteDuration = _ => 5.millis) extends LedgerCalls:
    val ledger: SimTigerBeetle = new SimTigerBeetle(Period(0))
    ledger.createAccounts(
      1L,
      Vector(
        Ledger.NewAccount(Funded, AccountLedger, 70),
        Ledger.NewAccount(Merchant, AccountLedger, 70),
        Ledger.NewAccount(Source, AccountLedger, 70),
        Ledger.NewAccount(ledgerConfig.fenceDebit, ledgerConfig.fenceLedger, 90),
        Ledger.NewAccount(ledgerConfig.fenceCredit, ledgerConfig.fenceLedger, 90),
      ),
    )
    def submit(call: Ledger.Submit): IO[Ledger.Result] =
      IO.sleep(after(call)) >> IO.realTime.map(t => Ledger.Result.Returned(ledger.createTransfers(t.toNanos + 1L, call.transfers)))

  val silentLog: LogWriter = new LogWriter:
    def append(append: CommandLog.Append): IO[CommandLog.Written] = IO.never
    def leaders: IO[CommandLog.Leaders] = IO.pure(CommandLog.Leaders(Set.empty))

  val silentLedger: LedgerCalls = (_: Ledger.Submit) => IO.never

  /** A member of the simulated group, polled as often as a real consumer would be. */
  final class Reader(kafka: SimKafka, every: FiniteDuration = 50.millis) extends LogReader:
    private val member = kafka.join(0L)
    var commits: Vector[CommandLog.Commit] = Vector.empty
    def deliveries: Stream[IO, CommandLog.Delivery] =
      Stream.repeatEval(IO.sleep(every) >> IO.realTime.map(t => kafka.poll(member, t.toMillis))).flatMap(Stream.emits)
    def commit(commit: CommandLog.Commit): IO[Boolean] =
      IO:
        commits :+= commit
        kafka.commit(member, commit)
