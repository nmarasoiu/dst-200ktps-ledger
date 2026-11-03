package ledger.contract

import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all.*
import ledger.core.*
import ledger.core.CommandLog.{Delivery, Written}
import ledger.shell.{KafkaLog, Provision}
import munit.{AnyFixture, CatsEffectSuite}
import scala.concurrent.duration.*

/** The resolver's reader on a broker that stops answering for longer than a request may take. It holds nothing a restart
  * must restore, so stopping would be safe, but a stall of the brokers would then stop every resolver at once.
  */
class Fs2KafkaOutageSuite extends CatsEffectSuite:
  private val run = sys.env.contains("CI") || RealTigerBeetle.dockerAvailable
  private val real = ResourceSuiteLocalFixture("kafka", RealKafka.pausableBroker)

  override def munitFixtures: Seq[AnyFixture[?]] = if run then Seq(real) else Seq.empty
  override def munitTests(): Seq[Test] = if run then super.munitTests() else Seq.empty
  override def munitIOTimeout: Duration = 3.minutes

  private val Record1 = Record.CommandRecord(Command.Cancel(ClientKey(WallTime(1_700_000_000_000L), "outage")))

  for protocol <- Vector("consumer", "classic") do
    test(s"under the $protocol protocol, a reader goes on through a broker that stops answering, and reads what comes after"):
      val broker = real()
      val cfg = KafkaLog.Config(
        broker.bootstrap,
        topic = s"outage-$protocol",
        group = s"outage-$protocol",
        sendTimeout = 5.seconds,
        groupProtocol = protocol,
        consumerProperties = Map("request.timeout.ms" -> "1500", "default.api.timeout.ms" -> "2000"),
      )
      Provision.admin(broker.bootstrap).use(a => Provision.createLog(a, Provision.LogSpec(cfg.topic, 2, 1, 1)) >> Provision.seedOffsets(a, cfg.topic, cfg.group)) >>
        (KafkaLog.writer(cfg), KafkaLog.reader(cfg)).tupled.use: (log, reader) =>
          for
            seen <- Ref[IO].of(Vector.empty[Delivery])
            stopped <- Deferred[IO, Throwable]
            reading <- reader.deliveries.evalMap(d => seen.update(_ :+ d)).compile.drain.handleErrorWith(e => stopped.complete(e).void).start
            _ <- until(stopped, seen, "the end of a partition", 120)(_.exists(_.isInstanceOf[Delivery.ReachedEnd]))
            _ <- broker.pause >> IO.sleep(5.seconds) >> broker.resume
            _ <- appended(log)
            _ <- until(stopped, seen, "the record written after", 240)(_.exists { case Delivery.Read(_, _, _, r) => r == Record1; case _ => false })
            _ <- reading.cancel
          yield ()

  private def until(stopped: Deferred[IO, Throwable], seen: Ref[IO, Vector[Delivery]], what: String, left: Int)(holds: Vector[Delivery] => Boolean): IO[Unit] =
    (stopped.tryGet, seen.get).tupled.flatMap:
      case (Some(e), _) => IO(fail(s"the reader stopped while waiting for $what: $e"))
      case (_, got) if holds(got) => IO.unit
      case (_, got) if left == 0 => IO(fail(s"never saw $what: $got"))
      case _ => IO.sleep(250.millis) >> until(stopped, seen, what, left - 1)(holds)

  private def appended(log: ledger.shell.LogWriter): IO[Unit] =
    log.append(CommandLog.Append(Record1)).flatMap:
      case Written.Acknowledged => IO.unit
      case _ => IO.sleep(250.millis) >> appended(log)
