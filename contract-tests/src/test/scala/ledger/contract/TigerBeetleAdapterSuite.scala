package ledger.contract

import cats.effect.IO
import cats.syntax.all.*
import com.tigerbeetle.CreateTransferStatus
import ledger.core.*
import ledger.shell.TigerBeetle
import munit.{AnyFixture, CatsEffectSuite}
import scala.concurrent.duration.*

/** What only the real adapter has to get right: the Java client under it. */
class TigerBeetleAdapterSuite extends CatsEffectSuite:
  private val run = sys.env.contains("CI") || RealTigerBeetle.dockerAvailable
  private val real = ResourceSuiteLocalFixture("tigerbeetle", RealTigerBeetle.reachable)

  override def munitFixtures: Seq[AnyFixture[?]] = if run then Seq(real) else Seq.empty
  override def munitIOTimeout: Duration = 2.minutes

  // Needs no server. The core's two deprecated codes are ones the client no longer has.
  test("the core and the client know the same status codes"):
    val client = CreateTransferStatus.values.toVector.map(s => Integer.toUnsignedLong(s.value)).toSet
    val core = TbStatus.values.toVector.filterNot(s => s == TbStatus.DeprecatedOk || s == TbStatus.Deprecated18).map(_.code).toSet
    assertEquals(client, core)
    CreateTransferStatus.values.foreach(s => assertEquals(TigerBeetle.statusOf(s.value).map(_.code), Some(Integer.toUnsignedLong(s.value))))

  private val cfg = LedgerConfig(AccountId.of(9001), AccountId.of(9002), 99, 90, 71)

  private def fence(n: Int): Transfer =
    Transfers.fence(Ids.authorization(ClientKey(WallTime(1_700_000_000_000L + n), s"adapter-$n")), cfg)

  private def setUp(tb: TigerBeetle): IO[Unit] =
    tb.createAccounts(
      Ledger.CreateAccounts(
        RealTigerBeetle.Cluster,
        Vector(Ledger.NewAccount(cfg.fenceDebit, cfg.fenceLedger, 90), Ledger.NewAccount(cfg.fenceCredit, cfg.fenceLedger, 90)),
      ),
    )

  // Were a cancelled wait to cancel the client's future, the client would halt this JVM and take the run with it.
  test("a fiber that stops waiting does not take the event back, and the process survives the late reply"):
    assume(run, "no Docker")
    val calls = (1 to 200).toVector.map(fence)
    RealTigerBeetle.adapterOf(real()).use: tb =>
      for
        _ <- setUp(tb)
        fibers <- calls.traverse(t => tb.submit(Ledger.Submit(RealTigerBeetle.Cluster, Vector(t))).start)
        _ <- IO.cede
        _ <- fibers.traverse_(_.cancel)
        _ <- IO.sleep(1.second)
        replays <- calls.traverse(t => tb.submit(Ledger.Submit(RealTigerBeetle.Cluster, Vector(t))))
        landed = replays.count(_ == Ledger.Result.Returned(Vector(TbStatus.Exists)))
        _ <- IO.println(s"TigerBeetleAdapterSuite: $landed of ${calls.size} cancelled waits had already handed over their event")
        _ = assert(landed > 0, "no wait was cancelled after its hand-off, so this checks nothing")
      yield ()

  test("a call on a closed client fails without statuses, as a call whose effect is unknown"):
    assume(run, "no Docker")
    RealTigerBeetle.adapterOf(real()).allocated.flatMap: (tb, close) =>
      close >> tb.submit(Ledger.Submit(RealTigerBeetle.Cluster, Vector(fence(1000)))).assertEquals(Ledger.Result.Failed)
