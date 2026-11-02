package ledger.shell

import cats.effect.std.{Queue, Supervisor}
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import ledger.core.*
import scala.concurrent.duration.*

/** A resolver instance: it runs the resolver's machine on what the consumer reports (README section 8, resolver). It
  * holds nothing a restart must restore: reading again from the committed offset rebuilds its state (README section 8,
  * failover).
  */
object ResolverProcess:

  /** Runs until something stops it, and fails rather than go on past a command it could not read (README section 4,
    * command log rules).
    *
    * Commits are coalesced, one per partition per `commitEvery`, since each is a round trip that holds up the consumer.
    * Committing later only means reading more again after a restart. Whichever commit lands is safe: every command
    * below it has an outcome in the log.
    */
  def run(
      cfg: ResolverPartition.Config,
      reader: LogReader,
      log: LogWriter,
      ledger: LedgerCalls,
      alarm: Alarm => IO[Unit],
      commitEvery: FiniteDuration = 1.second,
  ): IO[Unit] =
    Supervisor[IO].use: supervisor =>
      for
        events <- Queue.bounded[IO, Resolver.Event](MachineLoop.EventCapacity)
        pending <- Ref[IO].of(Map.empty[Partition, Offset])
        now = IO.realTime.map(t => ResolverTime(t.toMillis))
        perform = (f: Resolver.Effect) =>
          f match
            case Resolver.Effect.Submit(ref, submit) =>
              supervisor.supervise(ledger.submit(submit).flatMap(result => events.offer(Resolver.Event.Returned(ref, result)))).void
            case Resolver.Effect.Append(ref, append) =>
              supervisor.supervise(log.append(append).flatMap(written => events.offer(Resolver.Event.Written(ref, written)))).void
            case Resolver.Effect.Commit(commit) => pending.update(_.updated(commit.partition, commit.next))
            case Resolver.Effect.WakeAt(at) =>
              val wake = IO.realTime.flatMap(t => IO.sleep((at.millis - t.toMillis).max(0L).millis)) >> events.offer(Resolver.Event.TimeAdvanced)
              supervisor.supervise(wake).void
            case Resolver.Effect.Raise(raised) => alarm(raised)
        steps = MachineLoop.run(Resolver.machine(cfg), now, events, perform)
        reading = reader.deliveries
          .evalTap:
            case CommandLog.Delivery.Revoked(p) => pending.update(_ - p)
            case _ => IO.unit
          .evalMap(delivery => events.offer(Resolver.Event.Delivered(delivery)))
          .compile
          .drain
        committing = (IO.sleep(commitEvery) >> commitAll(reader, pending)).foreverM
        _ <- (steps, reading, committing).parTupled
      yield ()

  private def commitAll(reader: LogReader, pending: Ref[IO, Map[Partition, Offset]]): IO[Unit] =
    pending.getAndSet(Map.empty).flatMap(_.toVector.traverse_((p, next) => reader.commit(CommandLog.Commit(p, next))))
