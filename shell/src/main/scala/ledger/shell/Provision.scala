package ledger.shell

import cats.effect.{IO, Resource}
import java.util.Properties
import ledger.core.*
import org.apache.kafka.clients.admin.{Admin, NewTopic, OffsetSpec}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.{LeaderNotAvailableException, TopicExistsException, UnknownTopicOrPartitionException}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** What the design assumes of the command log and the ledger, set up once and checked at every start (README
  * section 4, command log rules; section 8, the cost). Startup fails if the live system disagrees, since a topic left
  * to compact or to expire early would lose commands without a sign.
  */
object Provision:

  /** The command log as the design runs it. Replication comes down only on a laptop's single broker. */
  final case class LogSpec(
      topic: String,
      partitions: Int,
      replication: Short = 3,
      minInSync: Int = 2,
      retention: FiniteDuration = 30.days,
  ):
    def configs: Map[String, String] = Map(
      "cleanup.policy" -> "delete",
      "message.timestamp.type" -> "LogAppendTime",
      "min.insync.replicas" -> minInSync.toString,
      "retention.ms" -> retention.toMillis.toString,
      "retention.bytes" -> "-1",
    )

  def admin(bootstrap: String): Resource[IO, Admin] =
    Resource.fromAutoCloseable:
      IO.blocking:
        val p = new Properties
        p.put("bootstrap.servers", bootstrap)
        Admin.create(p)

  /** Creates the topic if it is not there. It never changes one that is: `check` says if that one is wrong. */
  def createLog(admin: Admin, spec: LogSpec): IO[Unit] =
    IO.blocking:
      val topic = new NewTopic(spec.topic, spec.partitions, spec.replication).configs(spec.configs.asJava)
      try admin.createTopics(List(topic).asJava).all.get: Unit
      catch case e: java.util.concurrent.ExecutionException if e.getCause.isInstanceOf[TopicExistsException] => ()

  /** What disagrees with the design, in words an operator can act on. */
  def logProblems(admin: Admin, spec: LogSpec): IO[Vector[String]] =
    IO.blocking:
      val described = admin.describeTopics(List(spec.topic).asJava).allTopicNames.get.get(spec.topic)
      val partitions = described.partitions.asScala.toVector
      val shape = Vector(
        Option.when(partitions.size != spec.partitions)(s"${spec.topic} has ${partitions.size} partitions, not ${spec.partitions}"),
        partitions.find(_.replicas.size != spec.replication.toInt).map(i => s"${spec.topic} partition ${i.partition} has ${i.replicas.size} replicas, not ${spec.replication}"),
      ).flatten
      val resource = new ConfigResource(ConfigResource.Type.TOPIC, spec.topic)
      val live = admin.describeConfigs(List(resource).asJava).all.get.get(resource)
      val configs = spec.configs.toVector.sortBy(_._1).flatMap: (name, wanted) =>
        val value = live.get(name).value
        Option.when(value != wanted)(s"${spec.topic} has $name=$value, not $wanted")
      val broker = admin.describeCluster.nodes.get.asScala.head
      val brokerResource = new ConfigResource(ConfigResource.Type.BROKER, broker.idString)
      val offsets = admin.describeConfigs(List(brokerResource).asJava).all.get.get(brokerResource).get("offsets.retention.minutes").value.toLong
      val kept = Option.when(offsets.minutes < spec.retention)(s"brokers keep committed offsets for $offsets minutes, less than the log's ${spec.retention}")
      shape ++ configs ++ kept

  /** Puts the group at the start of the topic where it has no commit yet, as an operator does on first deployment: the
    * resolver's consumer stops rather than guess (README section 4, command log rules).
    */
  def seedOffsets(admin: Admin, topic: String, group: String): IO[Unit] =
    untilSettled:
      val described = admin.describeTopics(List(topic).asJava).allTopicNames.get.get(topic)
      val all = described.partitions.asScala.toVector.map(i => new TopicPartition(topic, i.partition))
      val committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata.get.asScala
      val missing = all.filterNot(committed.contains)
      if missing.nonEmpty then
        val earliest = admin.listOffsets(missing.map(_ -> OffsetSpec.earliest).toMap.asJava).all.get.asScala
        val offsets = earliest.map((tp, info) => tp -> new OffsetAndMetadata(info.offset)).toMap
        admin.alterConsumerGroupOffsets(group, offsets.asJava).all.get: Unit

  /** A new topic takes a moment to reach every broker, and a leader takes a moment to be elected. */
  private def untilSettled(body: => Unit): IO[Unit] =
    def go(left: Int): IO[Unit] =
      IO.blocking(body).handleErrorWith:
        case e: java.util.concurrent.ExecutionException
            if left > 0 && (e.getCause.isInstanceOf[UnknownTopicOrPartitionException] || e.getCause.isInstanceOf[LeaderNotAvailableException]) =>
          IO.sleep(200.millis) >> go(left - 1)
        case e => IO.raiseError(e)
    go(150)

  /** The fence accounts, set up in a cluster. Setting them up again changes nothing. */
  def createFenceAccounts(ledger: TigerBeetle, cluster: Period, cfg: LedgerConfig): IO[Unit] =
    ledger.createAccounts(Ledger.CreateAccounts(cluster, Vector(cfg.fenceDebit, cfg.fenceCredit).map(Ledger.NewAccount(_, cfg.fenceLedger, cfg.fenceCode))))

  def fenceProblems(ledger: TigerBeetle, cluster: Period, cfg: LedgerConfig): IO[Vector[String]] =
    val wanted = Vector(cfg.fenceDebit, cfg.fenceCredit)
    ledger.lookupAccounts(Ledger.LookupAccounts(cluster, wanted)).map: found =>
      val there = found.map(_.id).toSet
      wanted.filterNot(there).map(id => s"fence account ${id.value} is missing from cluster ${cluster.index}")

  /** Fails with every problem at once, so an operator fixes them in one go. */
  def requireNone(problems: Vector[String]): IO[Unit] =
    if problems.isEmpty then IO.unit else IO.raiseError(IllegalStateException(problems.mkString("Refusing to start: ", "; ", "")))
