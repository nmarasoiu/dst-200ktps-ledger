package ledger.contract

import cats.effect.std.Random
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import io.circe.jawn.parse
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Path}
import java.time.Duration as JDuration
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger
import ledger.core.*
import ledger.shell.*
import munit.{AnyFixture, CatsEffectSuite}
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry, RecordsToDelete}
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Stage 5's promise: end to end on real Kafka and a real TigerBeetle, surviving kills of any process (README section
  * 14). The edge and the resolver run as processes of their own, so a kill is a real `SIGKILL` and a pause a real
  * `SIGSTOP`. Callers follow the same machine the simulator's do, and each run is judged by the same invariants, over a
  * history made of what callers heard, what the log holds, and a scan of the ledger.
  */
class EndToEndSuite extends CatsEffectSuite:
  import EndToEndSuite.*

  private val run = sys.env.contains("CI") || RealTigerBeetle.dockerAvailable
  private val systems = ResourceSuiteLocalFixture(
    "kafka and tigerbeetle",
    for
      bootstrap <- RealKafka.broker
      address <- RealTigerBeetle.reachable
      ledger <- RealTigerBeetle.adapterOf(address)
    yield Systems(bootstrap, address, ledger),
  )

  override def munitFixtures: Seq[AnyFixture[?]] = if run then Seq(systems) else Seq.empty
  override def munitTests(): Seq[Test] = if run then super.munitTests() else Seq.empty
  override def munitIOTimeout: Duration = 5.minutes

  /** The whole picture, for what no single process knows. */
  private def judged(h: History): Vector[Violation] = Invariants.checkKeptContracts(h) ++ Liveness.check(h, Bounds)

  private def assertClean(run: Run, h: History): Unit =
    val found = judged(h)
    assert(found.isEmpty, s"${found.size} violations, first: ${found.take(5).mkString("\n")}\nProcess logs: ${run.logs}")

  test("nothing goes wrong, so the resolver has nothing to do, and every answer matches the ledger"):
    val r = newRun(systems())
    (for
      _ <- r.prepare
      h <- (r.edge(0), r.edge(1), r.resolver(0)).tupled.use: _ =>
        r.ready(2) >> r.load(keys = 40, pause = 75.millis) >> IO.sleep(Bounds.outcomeWithin.min(8.seconds)) >> r.history(None)
    yield
      assertClean(r, h)
      assert(h.caller.exists { case CallerRecord.Received(_, _, Reply.Decided(AuthOutcome.Approved)) => true; case _ => false }, h.caller)
      assert(h.ledger.transfers.exists(_.transfer.isPost), "no capture reached the ledger")
      assertEquals(h.log.collect { case LogRecord(_, _, _, Record.OutcomeRecord(c, _, Writer.Resolver)) => c }, Vector.empty[Command])
    ).guarantee(r.close)

  test("edges and resolvers killed under load: every command gets an outcome, and nothing breaks"):
    val r = newRun(systems())
    (for
      _ <- r.prepare
      edges <- Vector(0, 1).traverse(r.startEdge)
      resolvers <- Vector(0, 1).traverse(r.startResolver)
      _ <- r.ready(2)
      // Busy enough that a kill catches requests in flight.
      loading <- r.load(keys = 3000, pause = 10.millis).start
      // Kill each in turn, and bring it back: the load goes on through it.
      _ <- (Vector(edges(0), resolvers(0), edges(1), resolvers(1), edges(0), resolvers(1))).traverse_ : victim =>
        IO.sleep(3.seconds) >> victim.killAndRestart
      _ <- r.ready(2)
      stopped <- r.now
      _ <- loading.joinWithUnit
      _ <- IO.sleep(Bounds.outcomeWithin + 1.second)
      h <- r.history(Some(stopped))
      _ <- (edges ++ resolvers).traverse_(_.kill)
    yield
      assertClean(r, h)
      // The kills did what they were meant to: callers lost their answers, and the resolver finished what edges left.
      assert(h.caller.exists(_.isInstanceOf[CallerRecord.Lost]), "no caller lost an answer")
      assert(h.log.exists { case LogRecord(_, _, _, Record.OutcomeRecord(_, _, Writer.Resolver)) => true; case _ => false }, "the resolver never had to act")
    ).guarantee(r.close)

  test("a resolver restarted from the start of the log acts only on commands with no outcome: here, none"):
    val r = newRun(systems())
    (for
      _ <- r.prepare
      _ <- (r.edge(0), r.resolver(0)).tupled.use(_ => r.ready(1) >> r.load(keys = 30, pause = 40.millis) >> IO.sleep(8.seconds))
      before <- r.endOffsets
      _ = assert(before.values.sum > 0L, before)
      _ <- r.seekGroupToStart
      _ <- r.resolver(0).use(_ => IO.sleep(Grace + Settings.Deadline + 4.seconds))
      after <- r.endOffsets
      committed <- r.committed
    yield
      assertEquals(after, before, "a restarted resolver wrote to the log")
      assertEquals(committed, before, "it did not read to the end")
    ).guarantee(r.close)

  test("a resolver whose committed offset has aged out of the log stops with an error, rather than skip commands"):
    val r = newRun(systems())
    (for
      _ <- r.prepare
      _ <- r.edge(0).use(_ => r.ready(1) >> r.load(keys = 20, pause = 20.millis))
      _ <- r.deleteAllRecords
      process <- r.launch("ledger.shell.ResolverMain", Map.empty, "aged-out")
      code <- process.exitCode(60.seconds)
    yield assertEquals(code, Some(1), s"process log: ${r.logs}")
    ).guarantee(r.close)

  test("a resolver with no commit at all stops with an error, since nobody said where to start"):
    val r = newRun(systems())
    (for
      _ <- r.prepareWithoutSeeding
      process <- r.launch("ledger.shell.ResolverMain", Map.empty, "unseeded")
      code <- process.exitCode(60.seconds)
    yield assertEquals(code, Some(1), s"process log: ${r.logs}")
    ).guarantee(r.close)

  test("paused past their deadlines, a zombie edge and a zombie resolver wake to find a pipeline that moved on, and break nothing"):
    val r = newRun(systems())
    (for
      _ <- r.prepare
      edges <- Vector(0, 1).traverse(r.startEdge)
      resolvers <- Vector(0, 1).traverse(r.startResolver)
      _ <- r.ready(2)
      loading <- r.load(keys = 500, pause = 50.millis).start
      _ <- IO.sleep(3.seconds)
      _ <- edges(0).pause >> IO.sleep(4.seconds) >> edges(0).resume
      _ <- IO.sleep(2.seconds)
      _ <- resolvers(0).pause >> IO.sleep(6.seconds) >> resolvers(0).resume
      stopped <- r.now
      _ <- loading.joinWithUnit
      _ <- IO.sleep(Bounds.outcomeWithin + 1.second)
      h <- r.history(Some(stopped))
      _ <- (edges ++ resolvers).traverse_(_.kill)
    yield assertClean(r, h)
    ).guarantee(r.close)

  // Between every pair of steps (README section 5): each kill point, for each kind of command. The edge runs in this JVM
  // so a kill can land exactly there, and a real resolver finishes what it left.
  test("an edge killed between any two steps leaves only commands the resolver finishes"):
    val r = newRun(systems())
    (for
      _ <- r.prepare
      records <- Ref[IO].of(Vector.empty[CallerRecord])
      history <- (KafkaLog.writer(r.settings.kafka), r.resolver(0), r.resolver(1)).tupled.use: (log, _, _) =>
        val healthy = EdgeProcess.resource(r.settings.edge, log, r.ledger, _ => IO.unit)
        healthy.use: edge =>
          val cases = for
            kind <- Vector(Kind.Authorization, Kind.Cancel, Kind.Capture)
            point <- KillPoint.values.toVector
          yield (kind, point)
          cases.zipWithIndex.parTraverse_ : (c, i) =>
            r.killedAt(c._1, c._2, i, log, edge, records)
        .flatMap(_ => r.now.flatMap(stopped => IO.sleep(Bounds.outcomeWithin) >> records.get.flatMap(r.historyOf(_, Some(stopped)))))
    yield
      assertClean(r, history)
      assert(history.log.exists { case LogRecord(_, _, _, Record.OutcomeRecord(_, _, Writer.Resolver)) => true; case _ => false }, "the resolver never had to act")
    ).guarantee(r.close)

  test("an edge refuses to start on a command log that would lose commands, and names what is wrong"):
    val r = newRun(systems())
    val spec = r.settings.log
    val wrong = spec.copy(topic = spec.topic + "-wrong", partitions = 2, retention = 1.day)
    (for
      _ <- r.prepare
      problems <- Provision.admin(systems().bootstrap).use: admin =>
        for
          _ <- Provision.createLog(admin, wrong)
          _ <- r.setConfig(admin, wrong.topic, "cleanup.policy", "compact")
          onTopic <- Provision.logProblems(admin, spec.copy(topic = wrong.topic))
          onBroker <- Provision.logProblems(admin, spec.copy(retention = 60.days))
        yield onTopic ++ onBroker
      refused <- r.launch("ledger.shell.EdgeMain", Map("TOPIC" -> wrong.topic, "PORT" -> "18999"), "refused")
      code <- refused.exitCode(60.seconds)
      said <- r.logText
    yield
      assert(problems.exists(_.contains("2 partitions")), problems)
      assert(problems.exists(_.contains("cleanup.policy=compact")), problems)
      assert(problems.exists(_.contains("retention.ms")), problems)
      assert(problems.exists(_.contains("committed offsets")), problems)
      assertEquals(code, Some(1), said)
      assert(said.contains("Refusing to start"), said)
      assert(said.contains("cleanup.policy=compact"), said)
    ).guarantee(r.close)

  test("startup refuses a ledger without its fence accounts"):
    // A fresh cluster ID is not needed: these accounts are looked up by ID, and nothing here creates the wrong ones.
    val notThere = Settings.Fence.copy(fenceDebit = AccountId.of(123456789L))
    Provision.fenceProblems(systems().ledger, Settings.Cluster, notThere).map: problems =>
      assert(problems.exists(_.contains("is missing")), problems)

  private val counter = new AtomicInteger(0)
  private def newRun(s: Systems): Run = new Run(s, counter.incrementAndGet())

object EndToEndSuite:
  final case class Systems(bootstrap: String, ledgerAddress: String, ledger: TigerBeetle)

  val Grace: FiniteDuration = 3.seconds

  /** The broker runs in a container with its own clock: measured stamping records up to 2 ms after their acknowledgement
    * was heard here. An answer that came before its outcome by less than this goes unseen here; the simulator, with one
    * clock, is what holds that order exactly.
    */
  val LogClockSlack: FiniteDuration = 100.millis

  /** From what `Settings` gives the processes. A caller's answer may take a moment on a busy laptop. */
  val Bounds: Liveness.Bounds = Liveness.Bounds(
    deadline = Settings.Deadline,
    answerWithin = Settings.Deadline + 1.5.seconds,
    outcomeWithin = Settings.Deadline + Grace + 6.seconds,
    settle = 6.seconds,
    expiryLag = 1.second,
  )

  enum KillPoint derives CanEqual:
    case BeforeCommand, CommandLandedUnheard, BeforeLedgerCall, LedgerCallUnheard, OutcomeLandedUnheard

  private val AccountLedger = 7
  private val Partitions = 4
  private val classpath = sys.props.getOrElse("ledger.shell.classpath", "")
  private val ports = new AtomicInteger(18080)

  final class Launched(main: String, env: Map[String, String], log: Path):
    @volatile private var process: Process = start()

    private def start(): Process =
      val java = Path.of(sys.props("java.home"), "bin", "java").toString
      val builder = new ProcessBuilder(java, "--enable-native-access=ALL-UNNAMED", "-Xmx256m", "-cp", classpath, main)
      builder.environment.putAll(env.asJava)
      builder.redirectErrorStream(true)
      builder.redirectOutput(ProcessBuilder.Redirect.appendTo(log.toFile))
      builder.start()

    private def signal(name: String): IO[Unit] =
      IO.blocking(new ProcessBuilder("kill", s"-$name", process.pid.toString).start().waitFor()).void

    def kill: IO[Unit] = IO.blocking { process.destroyForcibly(); process.waitFor() }.void
    def killAndRestart: IO[Unit] = kill >> IO.sleep(500.millis) >> IO.blocking { process = start() }
    def pause: IO[Unit] = signal("STOP")
    def resume: IO[Unit] = signal("CONT")

    /** The exit code, or None if it is still running after `within`. */
    def exitCode(within: FiniteDuration): IO[Option[Int]] =
      IO.blocking(if process.waitFor(within.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS) then Some(process.exitValue) else None)

  /** One test's own topic, group, accounts and processes, on the shared Kafka and TigerBeetle. */
  final class Run(systems: Systems, n: Int):
    private val logDir = Files.createTempDirectory(s"e2e-$n-")
    private val debit = Vector.tabulate(3)(i => AccountId.of(100_000L * n + i))
    private val unfunded = AccountId.of(100_000L * n + 10)
    private val merchant = AccountId.of(100_000L * n + 11)
    private val source = AccountId.of(100_000L * n + 12)
    private val edgePorts = Vector.fill(2)(ports.getAndIncrement())
    private val launched = new java.util.concurrent.ConcurrentLinkedQueue[Launched]
    private val started = System.currentTimeMillis
    private val client = HttpClient.newBuilder.connectTimeout(JDuration.ofSeconds(1)).build
    private val recorded = new java.util.concurrent.ConcurrentLinkedQueue[CallerRecord]
    @volatile private var cursor = 0L

    val settings: Settings = Settings(
      systems.bootstrap,
      Vector(systems.ledgerAddress),
      topic = s"e2e-$n",
      partitions = Partitions,
      replication = 1,
      minInSync = 1,
      group = s"resolver-$n",
    )

    def ledger: TigerBeetle = systems.ledger
    def logs: String = s"$logDir"

    /** Everything the processes printed. */
    def logText: IO[String] = IO.blocking(Files.list(logDir).toList.asScala.toVector.map(Files.readString(_)).mkString("\n"))

    def setConfig(admin: org.apache.kafka.clients.admin.Admin, topic: String, name: String, value: String): IO[Unit] =
      IO.blocking:
        val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
        val op = new AlterConfigOp(new ConfigEntry(name, value), AlterConfigOp.OpType.SET)
        admin.incrementalAlterConfigs(Map(resource -> List(op).asJavaCollection).asJava).all.get: Unit

    private val env = Map(
      "KAFKA_BOOTSTRAP" -> systems.bootstrap,
      "LEDGER_ADDRESSES" -> systems.ledgerAddress,
      "TOPIC" -> settings.topic,
      "PARTITIONS" -> Partitions.toString,
      "REPLICATION" -> "1",
      "MIN_INSYNC" -> "1",
      "GROUP" -> settings.group,
    )

    def now: IO[HistoryTime] = IO.realTime.map(t => HistoryTime(t.toMillis - started))

    private val names = new AtomicInteger(0)

    def launch(main: String, extra: Map[String, String], name: String): IO[Launched] =
      IO.blocking:
        val made = new Launched(main, env ++ extra, logDir.resolve(s"$name-${names.incrementAndGet()}.log"))
        launched.add(made)
        made

    def startEdge(i: Int): IO[Launched] = launch("ledger.shell.EdgeMain", Map("PORT" -> edgePorts(i).toString), s"edge$i")
    def startResolver(i: Int): IO[Launched] = launch("ledger.shell.ResolverMain", Map.empty, s"resolver$i")

    def edge(i: Int): Resource[IO, Launched] = Resource.make(startEdge(i))(_.kill)
    def resolver(i: Int): Resource[IO, Launched] = Resource.make(startResolver(i))(_.kill)

    def close: IO[Unit] = launched.asScala.toVector.traverse_(_.kill)

    /** The topic, the fence accounts, the group's offsets, this test's accounts, and a cursor past what came before. */
    def prepare: IO[Unit] = Shell.provision(settings) >> accounts

    def prepareWithoutSeeding: IO[Unit] = Provision.admin(systems.bootstrap).use(Provision.createLog(_, settings.log)) >> accounts

    private def accounts: IO[Unit] =
      val cluster = Settings.Cluster
      for
        last <- lastTimestamp(0L)
        _ = cursor = last
        _ <- ledger.createAccounts(
          Ledger.CreateAccounts(
            cluster,
            (debit :+ unfunded).map(Ledger.NewAccount(_, AccountLedger, 70, debitsMustNotExceedCredits = true)) ++
              Vector(merchant, source).map(Ledger.NewAccount(_, AccountLedger, 70)),
          ),
        )
        funding = debit.zipWithIndex.map: (id, i) =>
          Transfer(TransferId(U128(n.toLong + 1L, i + 1L)), source, id, U128.of(1_000_000), TransferId.Zero, AccountLedger, 72, Set.empty, 0)
        result <- ledger.submit(Ledger.Submit(cluster, funding))
        _ <- result match
          case Ledger.Result.Returned(statuses) if statuses.forall(_ == TbStatus.Created) => IO.unit
          case other => IO.raiseError(IllegalStateException(s"funding failed: $other"))
      yield ()

    private def lastTimestamp(after: Long): IO[Long] =
      ledger.scan(Ledger.Scan(Settings.Cluster, after, 8000)).flatMap: page =>
        page.lastOption.fold(IO.pure(after))(t => lastTimestamp(t.timestamp))

    /** Until this many edges answer, and the resolvers have had time to join. */
    def ready(edges: Int): IO[Unit] =
      def up(port: Int, left: Int): IO[Unit] =
        val ask = HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:$port/health")).timeout(JDuration.ofSeconds(1)).GET().build
        IO.blocking(client.send(ask, HttpResponse.BodyHandlers.discarding()).statusCode).attempt.flatMap:
          case Right(200) => IO.unit
          case _ if left == 0 => IO.raiseError(IllegalStateException(s"no edge on $port; logs in $logDir"))
          case _ => IO.sleep(250.millis) >> up(port, left - 1)
      edgePorts.take(edges).traverse_(up(_, 240)) >> IO.sleep(2.seconds)

    private def sendOverHttp(rng: Random[IO]): Request => IO[Caller.Event] = request =>
      val (path, json) = EdgeApi.encode(request)
      for
        port <- rng.betweenInt(0, edgePorts.size).map(edgePorts(_))
        ask = HttpRequest
          .newBuilder(URI.create(s"http://127.0.0.1:$port/$path"))
          .timeout(JDuration.ofSeconds(3))
          .header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(json.noSpaces))
          .build
        answer <- IO.fromCompletableFuture(IO(client.sendAsync(ask, HttpResponse.BodyHandlers.ofString))).attempt
      yield answer.toOption.flatMap(r => parse(r.body).toOption).flatMap(EdgeApi.replyFrom(_).toOption) match
        case Some(reply) => Caller.Event.Replied(reply)
        case None => Caller.Event.Lost

    /** One send, as the history records it. */
    private def once(request: Request, send: Request => IO[Caller.Event], record: CallerRecord => IO[Unit]): IO[Caller.Event] =
      for
        at <- now
        _ <- record(CallerRecord.Sent(at, request))
        event <- send(request)
        heard <- now
        _ <- event match
          case Caller.Event.Replied(reply) => record(CallerRecord.Received(heard, request, reply))
          case _ => record(CallerRecord.Lost(heard, request))
      yield event

    /** One key's whole life, by the caller's machine, through `send`. */
    private def life(i: Int, rng: Random[IO], send: Request => IO[Caller.Event], record: CallerRecord => IO[Unit]): IO[Unit] =
      def go(step: Step[Caller.State, Caller.Effect], resends: Int, first: Boolean): IO[Unit] =
        step.effects.headOption match
          case Some(Caller.Effect.Send(request)) =>
            (if first then IO.unit else rng.betweenInt(50, 200).flatMap(ms => IO.sleep(ms.millis))) >>
              once(request, send, record).flatMap(event => go(Caller.step(step.state, event), resends, first = false))
          case None =>
            step.state.phase match
              case Caller.Phase.Refused if resends < 3 => go(Caller.step(step.state, Caller.Event.Resend), resends + 1, first = false)
              case Caller.Phase.Refused => go(Caller.step(step.state, Caller.Event.Stop), resends, first = false)
              case Caller.Phase.Approved =>
                rng.betweenInt(0, 10).flatMap:
                  case 0 | 1 => go(Caller.step(step.state, Caller.Event.Stop), resends, first = false)
                  case 2 | 3 | 4 => go(Caller.step(step.state, Caller.Event.Capture(CaptureAmount.Partial(1))), resends, first = false)
                  case _ => go(Caller.step(step.state, Caller.Event.Capture(CaptureAmount.Full)), resends, first = false)
              case _ => IO.unit

      for
        created <- IO.realTime
        which <- rng.betweenInt(0, 10)
        account = if which == 0 then unfunded else debit(which % debit.size)
        amount <- rng.betweenInt(2, 300)
        key = ClientKey(WallTime(created.toMillis), s"e2e-$n-$i")
        _ <- go(Caller.start(Request.Authorize(key, account, merchant, U128.of(amount.toLong), AccountLedger)), 0, first = true)
      yield ()

    /** Keys arriving `pause` apart, each living out its life on its own. */
    def load(keys: Int, pause: FiniteDuration): IO[Unit] =
      Random.scalaUtilRandomSeedInt[IO](n).flatMap: rng =>
        (0 until keys).toVector.parTraverse_ : i =>
          IO.sleep(pause * i.toLong) >> life(i, rng, sendOverHttp(rng), r => IO { recorded.add(r); () })

    /** One key whose edge dies at `point`, serving the command of this `kind`. The rest of its life goes through a healthy
      * edge, as a caller's retry would through the load balancer.
      */
    def killedAt(kind: Kind, point: KillPoint, i: Int, log: LogWriter, healthy: EdgeProcess, records: Ref[IO, Vector[CallerRecord]]): IO[Unit] =
      val record: CallerRecord => IO[Unit] = r => records.update(_ :+ r)
      Random.scalaUtilRandomSeedInt[IO](1000 + i).flatMap: rng =>
        val key = ClientKey(WallTime(System.currentTimeMillis), s"kill-$n-$i")
        val authorization: Request.Authorize = Request.Authorize(key, debit(i % debit.size), merchant, U128.of(50), AccountLedger)
        val askHealthy: Request => IO[Caller.Event] = r => healthy.ask(r).map(Caller.Event.Replied(_))
        def dying(request: Request): IO[Caller.Event] =
          for
            died <- IO.deferred[Unit]
            mortalLog = new LogWriter:
              def leaders: IO[CommandLog.Leaders] = log.leaders
              def append(a: CommandLog.Append): IO[CommandLog.Written] = (a.record, point) match
                case (_: Record.CommandRecord, KillPoint.BeforeCommand) => died.complete(()) >> IO.never
                case (_: Record.CommandRecord, KillPoint.CommandLandedUnheard) => log.append(a) >> died.complete(()) >> IO.never
                case (_: Record.OutcomeRecord, KillPoint.OutcomeLandedUnheard) => log.append(a) >> died.complete(()) >> IO.never
                case _ => log.append(a)
            mortalLedger: LedgerCalls = call =>
              point match
                case KillPoint.BeforeLedgerCall => died.complete(()) >> IO.never
                case KillPoint.LedgerCallUnheard => ledger.submit(call) >> died.complete(()) >> IO.never
                case _ => ledger.submit(call)
            answer <- EdgeProcess.resource(settings.edge, mortalLog, mortalLedger, _ => IO.unit).use(e => IO.race(died.get, e.ask(request)))
          yield answer match
            case Right(reply) => Caller.Event.Replied(reply)
            case Left(_) => Caller.Event.Lost
        val life = kind match
          // The first thing it sends goes to the edge that dies, and the caller's machine does the rest.
          case Kind.Authorization => follow(Caller.start(authorization), dying, askHealthy, record)
          // Approved through a healthy edge, then the cancel or the capture goes to the one that dies.
          case other =>
            val second: Request = if other == Kind.Cancel then Request.Cancel(key) else Request.Capture(key, CaptureAmount.Full)
            once(authorization, askHealthy, record) >> resendUntilAnswered(second, dying, askHealthy, record)
        rng.betweenInt(0, 50).flatMap(ms => IO.sleep(ms.millis)) >> life

    private def follow(step: Step[Caller.State, Caller.Effect], first: Request => IO[Caller.Event], rest: Request => IO[Caller.Event], record: CallerRecord => IO[Unit]): IO[Unit] =
      step.effects.headOption match
        case Some(Caller.Effect.Send(request)) =>
          once(request, first, record).flatMap(event => follow(Caller.step(step.state, event), rest, rest, record))
        case None =>
          step.state.phase match
            case Caller.Phase.Refused | Caller.Phase.Approved => follow(Caller.step(step.state, Caller.Event.Stop), first, rest, record)
            case _ => IO.unit

    private def resendUntilAnswered(request: Request, first: Request => IO[Caller.Event], rest: Request => IO[Caller.Event], record: CallerRecord => IO[Unit]): IO[Unit] =
      once(request, first, record).flatMap:
        case Caller.Event.Replied(Reply.Decided(_)) => IO.unit
        case _ => IO.sleep(200.millis) >> resendUntilAnswered(request, rest, rest, record)

    private def consumerProperties: Properties =
      val p = new Properties
      p.put("bootstrap.servers", systems.bootstrap)
      p

    private def partitionsOf: Vector[TopicPartition] = (0 until Partitions).toVector.map(new TopicPartition(settings.topic, _))

    private def withConsumer[A](body: KafkaConsumer[Array[Byte], Array[Byte]] => A): IO[A] =
      IO.blocking:
        val c = new KafkaConsumer(consumerProperties, new ByteArrayDeserializer, new ByteArrayDeserializer)
        try body(c)
        finally c.close()

    def endOffsets: IO[Map[Partition, Long]] =
      withConsumer(_.endOffsets(partitionsOf.asJava).asScala.toMap.map((tp, o) => Partition(tp.partition) -> o.toLong))

    def committed: IO[Map[Partition, Long]] =
      Provision.admin(systems.bootstrap).use: admin =>
        IO.blocking:
          admin.listConsumerGroupOffsets(settings.group).partitionsToOffsetAndMetadata.get.asScala.toMap.map((tp, o) => Partition(tp.partition) -> o.offset)

    /** As an operator would, with no member in the group. It takes a moment for a killed member to leave. */
    def seekGroupToStart: IO[Unit] =
      Provision.admin(systems.bootstrap).use: admin =>
        val start = partitionsOf.map(_ -> new OffsetAndMetadata(0L)).toMap.asJava
        def go(left: Int): IO[Unit] =
          IO.blocking(admin.alterConsumerGroupOffsets(settings.group, start).all.get).void.handleErrorWith: e =>
            if left == 0 then IO.raiseError(e) else IO.sleep(500.millis) >> go(left - 1)
        go(60)

    /** Retention at work: everything before the group's commit leaves the log. */
    def deleteAllRecords: IO[Unit] =
      for
        ends <- endOffsets
        _ <- Provision.admin(systems.bootstrap).use: admin =>
          IO.blocking:
            val deleting = partitionsOf.map(tp => tp -> RecordsToDelete.beforeOffset(ends(Partition(tp.partition)))).toMap.asJava
            admin.deleteRecords(deleting).all.get
      yield ()

    private def readLog: IO[Vector[LogRecord]] =
      withConsumer: c =>
        val all = partitionsOf.asJava
        val ends = c.endOffsets(all)
        c.assign(all)
        c.seekToBeginning(all)
        val read = Vector.newBuilder[LogRecord]
        while partitionsOf.exists(tp => c.position(tp) < ends.get(tp)) do
          c.poll(JDuration.ofMillis(200)).asScala.foreach: r =>
            val record = RecordCodec.decode(r.value).fold(e => throw IllegalStateException(e), identity)
            read += LogRecord(Partition(r.partition), Offset(r.offset), HistoryTime(r.timestamp - started), record)
        read.result()

    private def scanAll(after: Long): IO[Vector[LedgerTransfer]] =
      ledger.scan(Ledger.Scan(Settings.Cluster, after, 8000)).flatMap: page =>
        page.lastOption.fold(IO.pure(Vector.empty[LedgerTransfer]))(t => scanAll(t.timestamp).map(page ++ _))

    def history(faultsStopped: Option[HistoryTime]): IO[History] = historyOf(recorded.asScala.toVector, faultsStopped)

    def historyOf(caller: Vector[CallerRecord], faultsStopped: Option[HistoryTime]): IO[History] =
      for
        log <- readLog
        transfers <- scanAll(cursor)
        ended <- now
      yield
        val accounts = (debit :+ unfunded).map(LedgerAccount(Settings.Cluster, _, debitsMustNotExceedCredits = true, creditsMustNotExceedDebits = false))
        val last = transfers.lastOption.fold(cursor)(_.timestamp)
        History(caller.sortBy(_.at.millis), log, Vector.empty, LedgerSnapshot(transfers, accounts, Map(Settings.Cluster -> last), None), faultsStopped, ended, LogClockSlack)
