package ledger.shell

import cats.effect.IO
import fs2.Stream
import ledger.core.*

/** The ports of README section 11 as the shell performs them. Each has a real adapter and a simulated one, so a shell
  * test can run the wiring with no broker and no cluster.
  */
trait LogWriter:
  /** Appends a record and waits for the in-sync replicas. Never fails: a write that went wrong may still land, so all
    * the machines need to hear is that it was not acknowledged (README section 5).
    */
  def append(append: CommandLog.Append): IO[CommandLog.Written]

  /** The partitions the producer's metadata shows without a leader (README section 8, failover). */
  def leaders: IO[CommandLog.Leaders]

trait LogReader:
  /** What the consumer tells the resolver, for as long as the stream runs. It fails rather than skip a command: on an
    * offset that is missing or has aged out of the log (README section 4, command log rules), or on a record it cannot
    * read. A broker that is away is not a failure: the stream waits for it.
    */
  def deliveries: Stream[IO, CommandLog.Delivery]

  /** Whether the coordinator took the commit. A zombie's is rejected, and the resolver needs no reply (README
    * section 5, zombie resolver).
    */
  def commit(commit: CommandLog.Commit): IO[Boolean]

trait LedgerCalls:
  /** Never fails: a call that gets no statuses has an unknown effect. */
  def submit(call: Ledger.Submit): IO[Ledger.Result]
