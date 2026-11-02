package ledger.shell

import Simulated.*
import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import ledger.core.*
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** The edge's wiring, on simulated time and simulated ports (README section 11, TestControl). The rules are the core's
  * and hold elsewhere: these check that the shell performs each effect, when it should, and joins each reply to the
  * right request.
  */
class EdgeProcessSuite extends CatsEffectSuite:

  private def timed[A](body: IO[A]): IO[(FiniteDuration, A)] = IO.monotonic.flatMap(start => body.flatMap(a => IO.monotonic.map(end => (end - start, a))))

  private def inTime[A](body: IO[A]): IO[A] = TestControl.executeEmbed(body)

  test("an authorization is answered once its two writes and its call have finished, and no sooner"):
    val log = new Log
    inTime:
      EdgeProcess.resource(edge(), log, new Tb, noAlarm).use(e => timed(e.ask(authorize(1))))
    .map: (took, reply) =>
      assertEquals(reply, Reply.Decided(AuthOutcome.Approved))
      assertEquals(took, 15.millis)
      assertEquals(log.records.map(_.command.kind), Vector(Kind.Authorization, Kind.Authorization))
      assert(log.records.last.isInstanceOf[Record.OutcomeRecord])

  test("a ledger that never answers: timed out at exactly the deadline, and the edge's own cancel is logged"):
    val log = new Log
    inTime:
      EdgeProcess.resource(edge(), log, silentLedger, noAlarm).use(e => timed(e.ask(authorize(1))) <* IO.sleep(100.millis))
    .map: (took, reply) =>
      assertEquals(reply, Reply.TimedOut)
      assertEquals(took, 1500.millis)
      assert(log.records.exists(_.command.kind == Kind.Cancel), log.records)

  test("a log that never acknowledges: the authorization is refused by its hand-off budget, and the ledger is never called"):
    var called = false
    val ledger: LedgerCalls = _ =>
      called = true
      IO.never
    inTime:
      EdgeProcess.resource(edge(), silentLog, ledger, noAlarm).use(e => timed(e.ask(authorize(1))) <* IO.sleep(5.seconds))
    .map: (took, reply) =>
      assert(reply.isInstanceOf[Reply.Refused], reply)
      assertEquals(took, 1.second)
      assert(!called)

  /** Fails every write of the kind `failing` picks, and lands the rest. */
  private def failingLog(real: Log, failing: Record => Boolean): LogWriter = new LogWriter:
    def append(append: CommandLog.Append): IO[CommandLog.Written] =
      if failing(append.record) then IO.pure(CommandLog.Written.Failed) else real.append(append)
    def leaders: IO[CommandLog.Leaders] = real.leaders

  test("a command's write that fails: the authorization is refused at once, and the ledger is never called"):
    var called = false
    val ledger: LedgerCalls = _ =>
      called = true
      IO.never
    val log = failingLog(new Log, _.isInstanceOf[Record.CommandRecord])
    inTime:
      EdgeProcess.resource(edge(), log, ledger, noAlarm).use(e => timed(e.ask(authorize(1))) <* IO.sleep(100.millis))
    .map: (took, reply) =>
      assertEquals(reply, Reply.Refused(RefusalReason.CommandNotAcknowledged))
      assertEquals(took, Duration.Zero)
      assert(!called)

  test("an outcome's write that fails: the caller hears a timeout, never the decision the log does not have"):
    val log = failingLog(new Log, _.isInstanceOf[Record.OutcomeRecord])
    inTime:
      EdgeProcess.resource(edge(), log, new Tb, noAlarm).use(e => e.ask(authorize(1)))
    .map(reply => assertEquals(reply, Reply.TimedOut))

  test("a ledger call that fails without statuses: the caller hears a timeout, since its effect is unknown"):
    val failing: LedgerCalls = _ => IO.pure(Ledger.Result.Failed)
    inTime:
      EdgeProcess.resource(edge(), new Log, failing, noAlarm).use(e => e.ask(authorize(1)))
    .map(reply => assertEquals(reply, Reply.TimedOut))

  test("a caller that gives up leaves its request running, so the log still gets its outcome"):
    val log = new Log
    inTime:
      EdgeProcess.resource(edge(), log, new Tb, noAlarm).use: e =>
        e.ask(authorize(1)).timeoutTo(1.milli, IO.pure(Reply.Retry)) <* IO.sleep(1.second)
    .map: reply =>
      assertEquals(reply, Reply.Retry)
      assert(log.records.exists(_.isInstanceOf[Record.OutcomeRecord]), log.records)

  test("a slow call holds up no other request"):
    val slowest = Ids.authorization(key(1))
    val tb = new Tb(call => if call.transfers.exists(_.id == slowest) then 1.second else 5.millis)
    inTime:
      EdgeProcess.resource(edge(), new Log, tb, noAlarm).use: e =>
        (timed(e.ask(authorize(1))), IO.sleep(1.milli) >> timed(e.ask(authorize(2)))).parTupled
    .map: (slow, fast) =>
      assertEquals(slow._1, 1010.millis)
      assertEquals(fast._1, 15.millis)
      assertEquals(fast._2, Reply.Decided(AuthOutcome.Approved))

  test("requests past the limit are refused at once, with no time spent"):
    inTime:
      EdgeProcess.resource(edge(maxInFlight = 2), new Log, silentLedger, noAlarm).use: e =>
        (e.ask(authorize(1)).start, e.ask(authorize(2)).start).tupled >> IO.sleep(1.milli) >> timed(e.ask(authorize(3)))
    .map: (took, reply) =>
      assertEquals(reply, Reply.Refused(RefusalReason.Overload))
      assertEquals(took, Duration.Zero)

  test("a partition that loses its leader: the edge hears within a poll, and refuses at once with nothing written"):
    val log = new Log
    val p = authorize(1).a.pipe(Partitioner.partition(_, Partitions))
    inTime:
      EdgeProcess.resource(edge(), log, new Tb, noAlarm).use: e =>
        IO.sleep(1.milli) >> IO(log.kafka.setLeader(p, present = false)) >> IO.sleep(300.millis) >> timed(e.ask(authorize(1)))
    .map: (took, reply) =>
      assertEquals(reply, Reply.Refused(RefusalReason.NoLeader))
      assertEquals(took, Duration.Zero)
      assertEquals(log.records, Vector.empty[Record])

  test("an alarm reaches whoever is told of alarms"):
    // A second copy of a key with a different amount is a caller's bug that the ledger reports.
    var alarms = Vector.empty[Alarm]
    val other = authorize(1).copy(amount = U128.of(11))
    inTime:
      EdgeProcess.resource(edge(), new Log, new Tb, a => IO { alarms :+= a }).use(e => e.ask(authorize(1)) >> e.ask(other))
    .map: reply =>
      assert(alarms.exists(_.isInstanceOf[Alarm.KeyReused]), alarms)
      assert(reply.isInstanceOf[Reply.Decided], reply)

  extension [A](a: A) private def pipe[B](f: A => B): B = f(a)
