package ledger.contract

import cats.effect.IO
import ledger.core.*
import scala.concurrent.duration.FiniteDuration

/** The ledger port as the suite sees it: the real adapter or the simulated one, one cluster, on its own clock. */
trait LedgerUnderTest:
  def createAccounts(accounts: Vector[Ledger.NewAccount]): IO[Unit]
  def submit(transfers: Vector[Transfer]): IO[Ledger.Result]
  def scan(after: Long, limit: Int): IO[Vector[LedgerTransfer]]
  def lookupAccounts(ids: Vector[AccountId]): IO[Vector[Ledger.Balances]]

  /** Lets this much time pass on the ledger's clock: a real wait, or the simulated clock moving on. */
  def passTime(d: FiniteDuration): IO[Unit]

  /** The ledger's clock, to measure how long something took. */
  def elapsed: IO[FiniteDuration]
