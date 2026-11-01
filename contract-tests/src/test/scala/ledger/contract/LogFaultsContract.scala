package ledger.contract

import cats.effect.IO
import cats.syntax.all.*
import java.util.concurrent.atomic.AtomicLong
import ledger.core.*
import ledger.core.CommandLog.{Leaders, Written}
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** What crashes do to writes, and what the design relies on about them (README sections 5, 6 and 8), held against a
  * real cluster and the simulated log alike.
  */
abstract class LogFaultsContract extends CatsEffectSuite:

  def log: FaultyLogUnderTest

  override def munitIOTimeout: Duration = 3.minutes

  private val counter = new AtomicLong(0)

  private def record(): Record =
    val n = counter.incrementAndGet()
    Record.CommandRecord(Command.Cancel(ClientKey(WallTime(1_700_000_000_000L + n), s"faults-$n")))

  private val P0 = Partition(0)

  /** An append that must go through. Right after a healing, a send may still time out, and may still land. */
  private def appendWhenHealed(topic: FaultyTopicUnderTest, r: Record): IO[Unit] =
    def go(left: Int): IO[Unit] =
      topic.append(r).flatMap:
        case Written.Acknowledged => IO.unit
        case Written.Failed if left > 0 => log.passTime(200.millis) >> go(left - 1)
        case Written.Failed => IO(fail(s"never acknowledged: $r"))
    go(100)

  private def eventually(what: String)(check: IO[Boolean]): IO[Unit] =
    def go(left: Int): IO[Unit] =
      check.flatMap: done =>
        if done then IO.unit
        else if left == 0 then IO(fail(s"never: $what"))
        else log.passTime(100.millis) >> go(left - 1)
    go(300)

  test("a write acknowledged before its leader fails survives, and the new leader takes more"):
    val (before, during) = (Vector.fill(3)(record()), record())
    for
      topic <- log.replicated
      _ <- before.traverse_(r => topic.append(r).assertEquals(Written.Acknowledged))
      _ <- topic.withLeaderFailed:
        for
          read <- topic.readAll
          _ = assertEquals(read.map(_.record), before)
          _ <- appendWhenHealed(topic, during)
        yield ()
      _ <- topic.readAll.map(_.map(_.record).distinct).assertEquals(before :+ during)
    yield ()

  test("a partition without a leader shows in the producer's metadata, and a write to it fails until it has one"):
    val (first, stranded, after) = (record(), record(), record())
    for
      topic <- log.unreplicated
      _ <- topic.append(first).assertEquals(Written.Acknowledged)
      _ <- topic.leaders.assertEquals(Leaders(Set.empty))
      _ <- topic.withoutLeader:
        for
          _ <- eventually("the producer sees no leader")(topic.leaders.map(_ == Leaders(Set(P0))))
          _ <- topic.append(stranded).assertEquals(Written.Failed)
        yield ()
      _ <- eventually("the producer sees a leader")(topic.leaders.map(_ == Leaders(Set.empty)))
      _ <- appendWhenHealed(topic, after)
      _ <- topic.readAll.map(_.map(_.record).last).assertEquals(after)
    yield ()

  test("with fewer replicas in sync than min.insync.replicas, a write fails and does not land"):
    val (first, refused, after) = (record(), record(), record())
    for
      topic <- log.strict
      _ <- topic.append(first).assertEquals(Written.Acknowledged)
      _ <- topic.withReplicaOutOfSync:
        topic.append(refused).assertEquals(Written.Failed)
      _ <- appendWhenHealed(topic, after)
      read <- topic.readAll
      _ = assert(!read.exists(_.record == refused), read)
      _ = assertEquals(read.map(_.offset), read.indices.toVector.map(i => Offset(i.toLong)))
      _ = assertEquals(read.last.record, after)
    yield ()

  test("a write whose send timed out may still land, and readers then see it like any other"):
    val r = record()
    for
      topic <- log.replicated
      _ <- topic.appendTimingOutThenLanding(r).assertEquals(Written.Failed)
      _ <- topic.endOffset.assertEquals(Offset(1))
      read <- topic.readAll
      _ = assertEquals(read.map(_.record), Vector(r))
      _ = assertEquals(read.map(_.offset), Vector(Offset(0)))
    yield ()

  test("a write not yet acknowledged may be lost with its leader, and the next record takes its offset"):
    val (kept, lost, next) = (record(), record(), record())
    for
      topic <- log.replicated
      _ <- topic.append(kept).assertEquals(Written.Acknowledged)
      _ <- topic.appendLostWithItsLeader(lost).assertEquals(Written.Failed)
      _ <- topic.endOffset.assertEquals(Offset(1))
      _ <- appendWhenHealed(topic, next)
      read <- topic.readAll
      _ = assert(!read.exists(_.record == lost), read)
      _ = assertEquals(read.map(_.record), Vector(kept, next))
      _ = assertEquals(read.map(_.offset), Vector(Offset(0), Offset(1)))
    yield ()
