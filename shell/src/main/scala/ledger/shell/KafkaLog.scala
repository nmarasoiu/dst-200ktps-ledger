package ledger.shell

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import fs2.Stream
import fs2.kafka.*
import ledger.core.*
import ledger.core.CommandLog.{Delivery, Written}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import scala.concurrent.duration.*
import scala.util.control.NonFatal

/** The command log port's real adapter, on fs2-kafka (README section 10): `acks=all`, and a delivery timeout near the
  * edge's deadline, so a record for a partition without a leader fails soon instead of filling the buffer that all
  * partitions share (README section 8, deadlines).
  */
object KafkaLog:

  final case class Config(
      bootstrap: String,
      topic: String,
      group: String,
      /** How long a write may stay unacknowledged. */
      sendTimeout: FiniteDuration,
      /** How often the producer's metadata refreshes, and so how soon the edge hears of a lost leader. */
      metadataEvery: FiniteDuration = 1.second,
      /** How often the consumer asks whether it has reached the end of a partition it owns. */
      endCheckEvery: FiniteDuration = 500.millis,
      /** "consumer" is the protocol the design prefers: a rebalance leaves unaffected partitions processing (README
        * section 6). The contract suites hold both.
        */
      groupProtocol: String = "consumer",
      /** Anything else the consumer should be told, such as a test's short timeouts. */
      consumerProperties: Map[String, String] = Map.empty,
  )

  def writer(cfg: Config): Resource[IO, LogWriter] =
    val millis = cfg.sendTimeout.toMillis.toString
    val settings = ProducerSettings[IO, Array[Byte], Array[Byte]]
      .withBootstrapServers(cfg.bootstrap)
      .withAcks(Acks.All)
      .withLinger(0.millis)
      .withProperties(
        "delivery.timeout.ms" -> millis,
        "request.timeout.ms" -> millis,
        "max.block.ms" -> millis,
        "metadata.max.age.ms" -> cfg.metadataEvery.toMillis.toString,
      )
    KafkaProducer.resource(settings).map: producer =>
      new LogWriter:
        def append(append: CommandLog.Append): IO[Written] =
          val record = ProducerRecord(cfg.topic, Partitioner.recordKey(append.record.command.a), RecordCodec.encode(append.record))
          producer.produce(ProducerRecords.one(record)).flatten.as(Written.Acknowledged).recover { case NonFatal(_) => Written.Failed }

        def leaders: IO[CommandLog.Leaders] =
          producer.partitionsFor(cfg.topic).map: partitions =>
            CommandLog.Leaders(partitions.filter(_.leader == null).map(i => Partition(i.partition)).toSet)

  def reader(cfg: Config): Resource[IO, LogReader] =
    // No auto.offset.reset here on purpose: fs2-kafka's default is `none`, which stops the resolver rather than skip
    // (README section 4, command log rules). The test that a member with no commit stops holds it there.
    val settings = ConsumerSettings[IO, Array[Byte], Array[Byte]]
      .withBootstrapServers(cfg.bootstrap)
      .withGroupId(cfg.group)
      .withEnableAutoCommit(false)
      .withProperties(cfg.consumerProperties + ("group.protocol" -> cfg.groupProtocol))
    KafkaConsumer.resource(settings).evalTap(_.subscribeTo(cfg.topic)).map(new Reader(cfg, _))

  private final class Reader(cfg: Config, consumer: KafkaConsumer[IO, Array[Byte], Array[Byte]]) extends LogReader:

    def deliveries: Stream[IO, Delivery] =
      consumer.partitionsMapStream
        .flatMap(assigned => Stream.emits(assigned.toVector.sortBy(_._1.partition)))
        .map((tp, records) => ofPartition(tp, records))
        .parJoinUnbounded

    private type Rec = CommittableConsumerRecord[IO, Array[Byte], Array[Byte]]

    /** A partition's stream ends when the partition is revoked. */
    private def ofPartition(tp: TopicPartition, records: Stream[IO, Rec]): Stream[IO, Delivery] =
      val p = Partition(tp.partition)
      Stream.eval(consumer.committed(Set(tp))).flatMap: committed =>
        val from = committed.get(tp).fold(0L)(_.offset)
        Stream.eval(Ref[IO].of(from)).flatMap: next =>
          val read = records.evalMap: r =>
            val record = r.record
            val appended = record.timestamp match
              case Timestamp.LogAppendTime(at) => Some(at)
              case _ => None
            for
              decoded <- IO.fromEither(RecordCodec.decode(record.value).leftMap(IllegalStateException(_)))
              at <- IO.fromOption(appended)(IllegalStateException(s"no append time at ${record.offset}: is the topic set to LogAppendTime?"))
              _ <- next.set(record.offset + 1)
            yield Delivery.Read(p, Offset(record.offset), LogTime(at), decoded)
          // A record read and not yet told still counts as not at the end, since `next` moves only as it is told.
          val ends = Stream
            .awakeEvery[IO](cfg.endCheckEvery)
            .evalMap(_ => (consumer.endOffsets(Set(tp)), next.get).mapN((ends, n) => Option.when(ends.get(tp).exists(n >= _))(Delivery.ReachedEnd(p))))
            .unNone
          Stream.emit(Delivery.Assigned(p, Offset(from))) ++ read.mergeHaltL(ends) ++ Stream.emit(Delivery.Revoked(p))

    def commit(commit: CommandLog.Commit): IO[Boolean] =
      consumer
        .commitSync(Map(new TopicPartition(cfg.topic, commit.partition.index) -> new OffsetAndMetadata(commit.next.value)))
        .as(true)
        .recover { case NonFatal(_) => false }
