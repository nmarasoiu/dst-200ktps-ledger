package ledger.shell

import cats.effect.{IO, IOApp, Resource}
import cats.syntax.all.*
import com.comcast.ip4s.{Host, Port}
import ledger.core.*
import org.http4s.ember.server.EmberServerBuilder
import scala.concurrent.duration.*

/** Everything a process needs to be told, from its environment. Only the addresses have no default: the rest is the
  * design's own, and a laptop's single broker turns replication down.
  */
final case class Settings(
    bootstrap: String,
    ledgerAddresses: Vector[String],
    ledgerCluster: U128 = U128.Zero,
    topic: String = "command-log",
    partitions: Int = 16,
    replication: Short = 3,
    minInSync: Int = 2,
    group: String = "resolver",
    groupProtocol: String = "consumer",
    port: Int = 8080,
    consumerProperties: Map[String, String] = Map.empty,
):
  def log: Provision.LogSpec = Provision.LogSpec(topic, partitions, replication, minInSync)

  def kafka: KafkaLog.Config =
    KafkaLog.Config(bootstrap, topic, group, Settings.SendTimeout, groupProtocol = groupProtocol, consumerProperties = consumerProperties)

  def clusters: Map[Period, TigerBeetle.Cluster] = Map(Settings.Cluster -> TigerBeetle.Cluster(ledgerCluster, ledgerAddresses))

  def edge: Edge.Config = Edge.Config(
    EdgeRequest.Config(Settings.Deadline, 1.second, 1.second, Settings.HoldTimeoutSeconds, PeriodSchedule.Single, Settings.Fence),
    Admission.Config(maxInFlight = 4096, maxLedgerLatency = 300.millis, partitions = partitions),
  )

  def resolver: ResolverPartition.Config =
    ResolverPartition.Config(Settings.Deadline, grace = 3.seconds, retryBackoff = 300.millis, PeriodSchedule.Single, Settings.Fence)

object Settings:
  val Deadline: FiniteDuration = 1500.millis
  val SendTimeout: FiniteDuration = 1500.millis
  val HoldTimeoutSeconds: Int = 7 * 24 * 3600
  val Cluster: Period = Period(0)

  /** Every fence must be identical, so these are fixed for all time (README section 8, fencing). The IDs sit far from
    * any a bank would give an account.
    */
  val Fence: LedgerConfig = LedgerConfig(
    fenceDebit = AccountId(U128(0x66656e6365000000L, 1L)),
    fenceCredit = AccountId(U128(0x66656e6365000000L, 2L)),
    fenceLedger = 999_999,
    fenceCode = 65_000,
    authorizationCode = 1_000,
  )

  def from(env: Map[String, String]): Either[String, Settings] =
    def required(name: String) = env.get(name).toRight(s"$name is not set")
    def number[A](name: String, default: A)(parse: String => Option[A]) =
      env.get(name).fold(Right(default))(text => parse(text).toRight(s"$name is not a number: $text"))
    (required("KAFKA_BOOTSTRAP"), required("LEDGER_ADDRESSES")).tupled.flatMap: (bootstrap, addresses) =>
      val defaults = Settings(bootstrap, addresses.split(",").toVector)
      for
        partitions <- number("PARTITIONS", defaults.partitions)(_.toIntOption)
        replication <- number("REPLICATION", defaults.replication)(_.toShortOption)
        minInSync <- number("MIN_INSYNC", defaults.minInSync)(_.toIntOption)
        port <- number("PORT", defaults.port)(_.toIntOption)
      yield defaults.copy(
        topic = env.getOrElse("TOPIC", defaults.topic),
        partitions = partitions,
        replication = replication,
        minInSync = minInSync,
        group = env.getOrElse("GROUP", defaults.group),
        groupProtocol = env.getOrElse("GROUP_PROTOCOL", defaults.groupProtocol),
        port = port,
      )

  def load: IO[Settings] = IO.fromEither(from(sys.env).leftMap(IllegalArgumentException(_)))

/** The three things an operator starts. */
object Shell:
  private val logAlarm: Alarm => IO[Unit] = alarm => IO.consoleForIO.errorln(s"ALARM $alarm")

  /** First deployment, and safe to run again: the topic, the fence accounts, and the group at the start of the topic. */
  def provision(s: Settings): IO[Unit] =
    (Provision.admin(s.bootstrap), TigerBeetle.resource(s.clusters)).tupled.use: (admin, ledger) =>
      Provision.createLog(admin, s.log) >>
        Provision.seedOffsets(admin, s.topic, s.group) >>
        Provision.createFenceAccounts(ledger, Settings.Cluster, Settings.Fence) >>
        check(admin, ledger, s)

  private def check(admin: org.apache.kafka.clients.admin.Admin, ledger: TigerBeetle, s: Settings): IO[Unit] =
    (Provision.logProblems(admin, s.log), Provision.fenceProblems(ledger, Settings.Cluster, Settings.Fence)).mapN(_ ++ _).flatMap(Provision.requireNone)

  private def checked(s: Settings, ledger: TigerBeetle): IO[Unit] = Provision.admin(s.bootstrap).use(check(_, ledger, s))

  def edge(s: Settings): Resource[IO, Unit] =
    for
      ledger <- TigerBeetle.resource(s.clusters)
      _ <- Resource.eval(checked(s, ledger))
      log <- KafkaLog.writer(s.kafka)
      process <- EdgeProcess.resource(s.edge, log, ledger, logAlarm)
      port <- Resource.eval(IO.fromOption(Port.fromInt(s.port))(IllegalArgumentException(s"bad port ${s.port}")))
      host <- Resource.eval(IO.fromOption(Host.fromString("0.0.0.0"))(IllegalArgumentException("bad host")))
      _ <- EmberServerBuilder.default[IO].withHost(host).withPort(port).withHttpApp(EdgeApi.routes(process).orNotFound).build
    yield ()

  def resolver(s: Settings): IO[Unit] =
    TigerBeetle.resource(s.clusters).use: ledger =>
      checked(s, ledger) >>
        (KafkaLog.reader(s.kafka), KafkaLog.writer(s.kafka)).tupled.use: (reader, log) =>
          ResolverProcess.run(s.resolver, reader, log, ledger, logAlarm)

/** Two accounts to try the pipeline with on a laptop, and money in the first. A bank's own accounts come from elsewhere. */
object LaptopAccountsMain extends IOApp.Simple:
  private val (customer, merchant, source) = (AccountId.of(1), AccountId.of(2), AccountId.of(3))
  private val AccountLedger = 7

  def run: IO[Unit] =
    Settings.load.flatMap: s =>
      TigerBeetle.resource(s.clusters).use: ledger =>
        val accounts = Vector(
          Ledger.NewAccount(customer, AccountLedger, 70, debitsMustNotExceedCredits = true),
          Ledger.NewAccount(merchant, AccountLedger, 70),
          Ledger.NewAccount(source, AccountLedger, 70),
        )
        val funding = Transfer(TransferId(U128.of(1)), source, customer, U128.of(1_000_000), TransferId.Zero, AccountLedger, 72, Set.empty, 0)
        ledger.createAccounts(Ledger.CreateAccounts(Settings.Cluster, accounts)) >> ledger.submit(Ledger.Submit(Settings.Cluster, Vector(funding))).void

object ProvisionMain extends IOApp.Simple:
  def run: IO[Unit] = Settings.load.flatMap(Shell.provision)

object EdgeMain extends IOApp.Simple:
  def run: IO[Unit] = Settings.load.flatMap(Shell.edge(_).useForever)

object ResolverMain extends IOApp.Simple:
  def run: IO[Unit] = Settings.load.flatMap(Shell.resolver)
