package ledger.shell

import Simulated.*
import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import fs2.Stream
import ledger.core.*
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** The resolver's wiring, on simulated time and simulated ports (README section 11, TestControl). */
class ResolverProcessSuite extends CatsEffectSuite:

  private def inTime[A](body: IO[A]): IO[A] = TestControl.executeEmbed(body)

  private def resolving[A](log: Log, reader: LogReader, tb: LedgerCalls)(body: IO[A]): IO[A] =
    ResolverProcess.run(resolver, reader, log, tb, noAlarm).background.use(_ => body)

  test("a command left without an outcome gets one a grace period after its deadline, in log time, and is then committed past"):
    val log = new Log
    val tb = new Tb
    val reader = new Reader(log.kafka)
    val cmd = command(1)
    val p = Partitioner.partition(cmd.a, Partitions)
    inTime:
      resolving(log, reader, tb)(log.append(CommandLog.Append(Record.CommandRecord(cmd))) >> IO.sleep(7.seconds))
    .map: _ =>
      val written = log.kafka.records(p)
      assertEquals(written.size, 2)
      assertEquals(written(1).record, Record.OutcomeRecord(cmd, AuthOutcome.Declined(DeclineReason.NotExecuted), Writer.Resolver))
      assert(written(1).appended - written(0).appended >= 4500.millis, written)
      // A fence holds `A`, and nothing else ever moved.
      assertEquals(tb.ledger.snapshot.transfers.map(_.transfer.id), Vector(cmd.a))
      assertEquals(log.kafka.committed(p), Some(Offset(2)))

  test("a command that has its outcome is left alone"):
    val log = new Log
    val tb = new Tb
    val cmd = command(1)
    val p = Partitioner.partition(cmd.a, Partitions)
    inTime:
      resolving(log, new Reader(log.kafka), tb):
        log.append(CommandLog.Append(Record.CommandRecord(cmd))) >>
          log.append(CommandLog.Append(Record.OutcomeRecord(cmd, AuthOutcome.Approved, Writer.Edge))) >>
          IO.sleep(10.seconds)
    .map: _ =>
      assertEquals(log.kafka.records(p).size, 2)
      assertEquals(tb.ledger.snapshot.transfers, Vector.empty)
      assertEquals(log.kafka.committed(p), Some(Offset(2)))

  test("a failed ledger call is tried again after its backoff"):
    val log = new Log
    val tb = new Tb
    var failures = 2
    val flaky: LedgerCalls = call =>
      if failures > 0 then IO { failures -= 1 }.as(Ledger.Result.Failed) else tb.submit(call)
    val cmd = command(1)
    inTime:
      resolving(log, new Reader(log.kafka), flaky)(log.append(CommandLog.Append(Record.CommandRecord(cmd))) >> IO.sleep(7.seconds))
    .map: _ =>
      assertEquals(failures, 0)
      assert(log.records.exists(_.isInstanceOf[Record.OutcomeRecord]), log.records)

  test("commits are coalesced, one per partition per interval, and the last one wins"):
    val log = new Log
    val reader = new Reader(log.kafka)
    val cmd = command(1)
    val p = Partitioner.partition(cmd.a, Partitions)
    val appendPairs = (1 to 40).toVector.traverse_ : _ =>
      log.append(CommandLog.Append(Record.CommandRecord(cmd))) >> log.append(CommandLog.Append(Record.OutcomeRecord(cmd, AuthOutcome.Approved, Writer.Edge)))
    inTime:
      resolving(log, reader, new Tb)(appendPairs >> IO.sleep(2.seconds))
    .map: _ =>
      val mine = reader.commits.filter(_.partition == p)
      assert(mine.size <= 3, mine)
      assertEquals(mine.last.next, Offset(80))

  test("a reader that fails stops the resolver with that failure, rather than go on past a command"):
    val failing: LogReader = new LogReader:
      def deliveries: Stream[IO, CommandLog.Delivery] = Stream.raiseError[IO](IllegalStateException("offset out of range"))
      def commit(commit: CommandLog.Commit): IO[Boolean] = IO.pure(true)
    inTime(ResolverProcess.run(resolver, failing, new Log, new Tb, noAlarm).attempt).map: result =>
      assert(result.left.exists(_.getMessage.contains("offset out of range")), result)
