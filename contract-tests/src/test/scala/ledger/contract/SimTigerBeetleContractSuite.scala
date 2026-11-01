package ledger.contract

import cats.effect.IO
import ledger.core.*
import ledger.sim.SimTigerBeetle
import scala.concurrent.duration.*

/** The contract against the simulated TigerBeetle. Time passes only when the suite says so, and a pulse runs every
  * 100 ms of it, as on an idle primary (README section 6).
  */
class SimTigerBeetleContractSuite extends LedgerContract:
  private val PulseEvery = 100.millis.toNanos
  private val tb = new SimTigerBeetle(Period(0))
  private var now = 1_000_000_000_000L
  private var nextPulse = now + PulseEvery

  // Several fibers call at once, as they do on the real client, and the simulator is not made for threads.
  private def atomically[A](body: => A): IO[A] = IO(tb.synchronized(body))

  val ledger: LedgerUnderTest = new LedgerUnderTest:
    def createAccounts(accounts: Vector[Ledger.NewAccount]): IO[Unit] = atomically(tb.createAccounts(now, accounts))
    def submit(transfers: Vector[Transfer]): IO[Ledger.Result] =
      atomically(Ledger.Result.Returned(tb.createTransfers(now, transfers)))
    def scan(after: Long, limit: Int): IO[Vector[LedgerTransfer]] = atomically(tb.scan(after, limit))
    def lookupAccounts(ids: Vector[AccountId]): IO[Vector[Ledger.Balances]] = atomically(tb.lookupAccounts(ids))
    def elapsed: IO[FiniteDuration] = atomically(now.nanos)
    def passTime(d: FiniteDuration): IO[Unit] = atomically:
      val end = now + d.toNanos
      while nextPulse <= end do
        now = nextPulse
        tb.pulse(now)
        nextPulse += PulseEvery
      now = end
