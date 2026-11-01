package ledger.contract

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import java.net.InetAddress
import java.time.Duration as JDuration
import java.util.Properties
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import ledger.core.*
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.clients.consumer.{CommitFailedException, ConsumerRebalanceListener, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{IllegalGenerationException, RebalanceInProgressException, UnknownMemberIdException}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startables
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** The Kafka clients against real brokers, set up as the design runs them: `acks=all`, append times from the leader
  * (README section 4, command log rules), and offsets only as the resolver commits them.
  */
object RealKafka:
  val Image = "apache/kafka:4.3.2"

  /** How a consumer speaks to the group coordinator: both protocols must reject a zombie (README section 6). */
  enum Protocol(val name: String) derives CanEqual:
    case Classic extends Protocol("classic")
    case Consumer extends Protocol("consumer")

  /** Only `A` matters to the broker, and the records of these suites are all cancels, so this is enough of a format.
    * The producer's own timestamp is old, so a test can tell it from the append time that replaces it.
    */
  private def encode(record: Record): Array[Byte] =
    val key = record.command.key
    s"${key.createdAt.millis}|${key.body}".getBytes("UTF-8")

  private def decode(bytes: Array[Byte]): Record =
    val text = new String(bytes, "UTF-8")
    val (millis, body) = text.splitAt(text.indexOf('|'))
    Record.CommandRecord(Command.Cancel(ClientKey(WallTime(millis.toLong), body.drop(1))))

  private val StaleProducerTimestamp = 1000L

  /** Shorter than the two seconds a paused broker stays in the in-sync replicas. */
  private val BriefSend = 1.second

  private val names = new AtomicLong(0)

  /** Clients of one cluster. `sendTimeout` is short when a test wants sends to time out. */
  final class Clients(bootstrap: String, protocol: Protocol, sendTimeout: FiniteDuration):

    private val admin: Admin =
      val p = new Properties
      p.put("bootstrap.servers", bootstrap)
      p.put("request.timeout.ms", "1000")
      p.put("default.api.timeout.ms", "3000")
      Admin.create(p)

    private val producers = new ConcurrentLinkedQueue[KafkaProducer[Array[Byte], Array[Byte]]]

    private def producer(timeout: FiniteDuration): KafkaProducer[Array[Byte], Array[Byte]] =
      val millis = timeout.toMillis.toString
      val p = new Properties
      p.put("bootstrap.servers", bootstrap)
      p.put("acks", "all")
      p.put("linger.ms", "0")
      p.put("delivery.timeout.ms", millis)
      p.put("request.timeout.ms", millis)
      p.put("max.block.ms", millis)
      p.put("metadata.max.age.ms", "200")
      val made = new KafkaProducer(p, new ByteArraySerializer, new ByteArraySerializer)
      producers.add(made)
      made

    def close: IO[Unit] =
      IO.blocking:
        producers.forEach(_.close(JDuration.ofSeconds(1)))
        admin.close()

    private def partition0(topic: String) = admin.describeTopics(List(topic).asJava).allTopicNames.get.get(topic).partitions.get(0)

    def replicasOf(topic: String): IO[Vector[Int]] = IO.blocking(partition0(topic).replicas.asScala.toVector.map(_.id))

    def inSync(topic: String): IO[Vector[Int]] = IO.blocking(partition0(topic).isr.asScala.toVector.map(_.id))

    def leaderOf(topic: String): IO[Option[Int]] = IO.blocking(Option(partition0(topic).leader).map(_.id).filter(_ >= 0))

    def newTopic(partitions: Int, replication: Short = 1, minInSync: Int = 1): IO[Topic] =
      val name = s"t${names.incrementAndGet()}"
      val topic = new NewTopic(name, partitions, replication)
        .configs(Map("message.timestamp.type" -> "LogAppendTime", "min.insync.replicas" -> minInSync.toString).asJava)
      IO.blocking(admin.createTopics(List(topic).asJava).all.get).as(new Topic(name, partitions))

    final class Topic(val name: String, val partitions: Int) extends TopicUnderTest:
      private val group = s"g-$name"

      private val patient = producer(sendTimeout)
      private val brief = producer(BriefSend)

      private def topicPartition(p: Partition) = new TopicPartition(name, p.index)

      private def sendWith(producer: KafkaProducer[Array[Byte], Array[Byte]], record: Record): IO[CommandLog.Written] =
        val key = Partitioner.recordKey(record.command.a)
        IO.async_[CommandLog.Written]: done =>
          val sent = new ProducerRecord[Array[Byte], Array[Byte]](name, null, StaleProducerTimestamp, key, encode(record))
          try
            producer.send(
              sent,
              (_, failure) => done(Right(if failure == null then CommandLog.Written.Acknowledged else CommandLog.Written.Failed)),
            ): Unit
          catch case _: org.apache.kafka.common.errors.TimeoutException => done(Right(CommandLog.Written.Failed))

      def append(record: Record): IO[CommandLog.Written] = sendWith(patient, record)

      /** A send whose timeout is shorter than a paused broker stays in the in-sync replicas. */
      def sendBriefly(record: Record): IO[CommandLog.Written] = sendWith(brief, record)

      /** The brief producer knows the leader before a broker goes away, so its send reaches the leader. */
      def knowLeader: IO[Unit] = IO.blocking(brief.partitionsFor(name)).void

      def leaderless: IO[Set[Partition]] =
        IO.blocking:
          patient.partitionsFor(name).asScala.toVector.filter(_.leader == null).map(i => Partition(i.partition)).toSet

      /** A consumer with no group, read only by offset. */
      private def plainConsumer: KafkaConsumer[Array[Byte], Array[Byte]] =
        val p = new Properties
        p.put("bootstrap.servers", bootstrap)
        p.put("request.timeout.ms", "1000")
        p.put("default.api.timeout.ms", "3000")
        new KafkaConsumer(p, new ByteArrayDeserializer, new ByteArrayDeserializer)

      def endOffset(partition: Partition): IO[Offset] =
        IO.blocking:
          val c = plainConsumer
          try Offset(c.endOffsets(List(topicPartition(partition)).asJava).get(topicPartition(partition)))
          finally c.close()

      def readAll(partition: Partition): IO[Vector[CommandLog.Delivery.Read]] =
        IO.blocking:
          val c = plainConsumer
          val tp = topicPartition(partition)
          try
            val end = c.endOffsets(List(tp).asJava).get(tp)
            c.assign(List(tp).asJava)
            c.seekToBeginning(List(tp).asJava)
            val read = Vector.newBuilder[CommandLog.Delivery.Read]
            while c.position(tp) < end do
              c.poll(JDuration.ofMillis(100)).asScala.foreach: r =>
                read += CommandLog.Delivery.Read(partition, Offset(r.offset), LogTime(r.timestamp), decode(r.value))
            read.result()
          finally c.close()

      def committed(partition: Partition): IO[Option[Offset]] =
        IO.blocking:
          val offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata.get
          Option(offsets.get(topicPartition(partition))).map(o => Offset(o.offset))

      def member: Resource[IO, MemberUnderTest] =
        Resource.make(IO.blocking(new Member))(m => IO.blocking(m.close())).widen

      private final class Member extends MemberUnderTest:
        private val consumer: KafkaConsumer[Array[Byte], Array[Byte]] =
          val p = new Properties
          p.put("bootstrap.servers", bootstrap)
          p.put("group.id", group)
          p.put("group.protocol", protocol.name)
          p.put("enable.auto.commit", "false")
          p.put("auto.offset.reset", "none")
          p.put("max.poll.interval.ms", Member.MaxPollInterval.toMillis.toString)
          if protocol == Protocol.Classic then
            p.put("session.timeout.ms", "1000")
            p.put("heartbeat.interval.ms", "100")
          new KafkaConsumer(p, new ByteArrayDeserializer, new ByteArrayDeserializer)

        private val told = mutable.ArrayBuffer.empty[CommandLog.Delivery]

        consumer.subscribe(
          List(name).asJava,
          new ConsumerRebalanceListener:
            def onPartitionsRevoked(lost: java.util.Collection[TopicPartition]): Unit =
              lost.asScala.foreach(tp => told += CommandLog.Delivery.Revoked(Partition(tp.partition)))
            override def onPartitionsLost(lost: java.util.Collection[TopicPartition]): Unit = onPartitionsRevoked(lost)
            def onPartitionsAssigned(gained: java.util.Collection[TopicPartition]): Unit =
              val committed = if gained.isEmpty then java.util.Map.of() else consumer.committed(new java.util.HashSet(gained))
              gained.asScala.foreach: tp =>
                val from = Option(committed.get(tp)).fold(0L)(_.offset)
                consumer.seek(tp, from)
                told += CommandLog.Delivery.Assigned(Partition(tp.partition), Offset(from))
          ,
        )

        def poll: IO[Vector[CommandLog.Delivery]] =
          IO.blocking:
            val records = consumer.poll(JDuration.ofMillis(100))
            records.asScala.foreach: r =>
              told += CommandLog.Delivery.Read(Partition(r.partition), Offset(r.offset), LogTime(r.timestamp), decode(r.value))
            val owned = consumer.assignment
            if !owned.isEmpty then
              val ends = consumer.endOffsets(owned)
              owned.asScala.toVector
                .filter(tp => consumer.position(tp) >= ends.get(tp))
                .foreach(tp => told += CommandLog.Delivery.ReachedEnd(Partition(tp.partition)))
            val out = told.toVector
            told.clear()
            out

        def commit(commit: CommandLog.Commit): IO[Boolean] =
          val offset = Map(topicPartition(commit.partition) -> new OffsetAndMetadata(commit.next.value)).asJava
          IO.blocking:
            try
              consumer.commitSync(offset)
              true
            catch
              case _: CommitFailedException | _: RebalanceInProgressException | _: IllegalGenerationException | _: UnknownMemberIdException =>
                false

        def close(): Unit = consumer.close()

      object Member:
        val MaxPollInterval: FiniteDuration = 1500.millis

  private def address(host: String, port: Int) = s"${InetAddress.getByName(host).getHostAddress}:$port"

  /** One broker, for what needs no replication. Fast group settings, so a rebalance takes a fraction of a second. */
  private val fastGroups = Map(
    "KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS" -> "500",
    "KAFKA_GROUP_CONSUMER_MIN_SESSION_TIMEOUT_MS" -> "1000",
    "KAFKA_GROUP_CONSUMER_SESSION_TIMEOUT_MS" -> "1000",
    "KAFKA_GROUP_CONSUMER_MIN_HEARTBEAT_INTERVAL_MS" -> "100",
    "KAFKA_GROUP_CONSUMER_HEARTBEAT_INTERVAL_MS" -> "100",
    "KAFKA_HEAP_OPTS" -> "-Xmx400m -Xms400m",
  )

  def singleBroker(protocol: Protocol): Resource[IO, LogUnderTest] =
    val broker = Resource.make(
      IO.blocking:
        val c = new org.testcontainers.kafka.KafkaContainer(Image)
        c.withEnv(fastGroups.asJava)
        c.start()
        c,
    )(c => IO.blocking(c.stop()))
    broker
      .flatMap: c =>
        val bootstrap = address(c.getHost, c.getMappedPort(9092))
        Resource.make(IO(new Clients(bootstrap, protocol, 10.seconds)))(_.close)
      .map: clients =>
        new LogUnderTest:
          def topic(partitions: Int): IO[TopicUnderTest] = clients.newTopic(partitions).map(t => t: TopicUnderTest)
          def passTime(d: FiniteDuration): IO[Unit] = IO.sleep(d)
          def now: IO[Long] = IO.realTime.map(_.toMillis)

  /** Three brokers, each also a controller, on one network. A paused broker is out of the cluster within two seconds,
    * by when its partitions have moved and it has left the in-sync replicas.
    */
  private val fastFailure = fastGroups ++ Map(
    "KAFKA_BROKER_SESSION_TIMEOUT_MS" -> "2000",
    "KAFKA_BROKER_HEARTBEAT_INTERVAL_MS" -> "200",
    "KAFKA_REPLICA_LAG_TIME_MAX_MS" -> "2000",
    "KAFKA_CONTROLLER_QUORUM_VOTERS" -> "1@k1:9094,2@k2:9094,3@k3:9094",
    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR" -> "3",
    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR" -> "3",
    "KAFKA_DEFAULT_REPLICATION_FACTOR" -> "3",
  )

  def threeBrokers: Resource[IO, FaultyLogUnderTest] =
    val brokers = Resource.make(
      IO.blocking:
        val network = Network.newNetwork()
        val all = (1 to 3).toVector.map: id =>
          val c = new org.testcontainers.kafka.KafkaContainer(Image)
          c.withNetwork(network)
          c.withNetworkAliases(s"k$id")
          c.withEnv(fastFailure.asJava)
          c.withEnv("KAFKA_NODE_ID", id.toString)
          id -> c
        Startables.deepStart(all.map(_._2).asJava).join()
        (all.toMap, network),
    ) { (all, network) =>
      IO.blocking:
        all.values.foreach(_.stop())
        network.close()
    }
    brokers.flatMap: (all, _) =>
      val bootstrap = all.values.toVector.map(c => address(c.getHost, c.getMappedPort(9092))).mkString(",")
      Resource.make(IO(new Clients(bootstrap, Protocol.Consumer, 5.seconds)))(_.close).map(new Cluster(all, _))

  private type Broker = org.testcontainers.kafka.KafkaContainer

  private final class Cluster(brokers: Map[Int, Broker], clients: Clients) extends FaultyLogUnderTest:
    private def pause(id: Int): IO[Unit] =
      IO.blocking(brokers(id).getDockerClient.pauseContainerCmd(brokers(id).getContainerId).exec()).void

    private def unpause(id: Int): IO[Unit] =
      IO.blocking(brokers(id).getDockerClient.unpauseContainerCmd(brokers(id).getContainerId).exec()).void

    /** Polls `check` until it holds. A broker being down makes a check fail for a while, which counts as not yet. */
    private def eventually(what: String)(check: IO[Boolean]): IO[Unit] =
      def go(left: Int): IO[Unit] =
        check.handleError(_ => false).flatMap: done =>
          if done then IO.unit
          else if left == 0 then IO.raiseError(IllegalStateException(s"never: $what"))
          else IO.sleep(100.millis) >> go(left - 1)
      go(300)

    def passTime(d: FiniteDuration): IO[Unit] = IO.sleep(d)

    def replicated: IO[FaultyTopicUnderTest] = faultyTopic(3, 2)
    def unreplicated: IO[FaultyTopicUnderTest] = faultyTopic(1, 1)
    def strict: IO[FaultyTopicUnderTest] = faultyTopic(3, 3)

    /** A fresh topic, with every replica in sync before a test takes a broker away. */
    private def faultyTopic(replication: Short, minInSync: Int): IO[FaultyTopicUnderTest] =
      clients.newTopic(1, replication, minInSync).flatMap: topic =>
        eventually("the topic ready")(clients.inSync(topic.name).map(_.size == replication.toInt)).as(new FaultyTopic(topic))

    private final class FaultyTopic(topic: clients.Topic) extends FaultyTopicUnderTest:
      private val P0 = Partition(0)
      private val name = topic.name

      private def leader: IO[Int] = clients.leaderOf(name).flatMap(l => IO.fromOption(l)(IllegalStateException("no leader")))

      private def followers: IO[Vector[Int]] =
        (leader, clients.replicasOf(name)).mapN((l, all) => all.filterNot(_ == l))

      private def allInSync: IO[Unit] =
        eventually("every replica back in sync"):
          (clients.inSync(name), clients.replicasOf(name)).mapN((isr, all) => isr.toSet == all.toSet)

      def append(record: Record): IO[CommandLog.Written] = topic.append(record)
      def endOffset: IO[Offset] = topic.endOffset(P0)
      def readAll: IO[Vector[CommandLog.Delivery.Read]] = topic.readAll(P0)
      def leaders: IO[CommandLog.Leaders] = topic.leaderless.map(CommandLog.Leaders(_))

      def withLeaderFailed[A](body: IO[A]): IO[A] =
        leader.flatMap: failed =>
          pause(failed).bracket(_ =>
            eventually("another replica leading")(clients.leaderOf(name).map(l => l.nonEmpty && !l.contains(failed))) >> body,
          )(_ => unpause(failed)) <* allInSync

      def withoutLeader[A](body: IO[A]): IO[A] =
        leader.flatMap: failed =>
          pause(failed).bracket(_ => eventually("no leader")(clients.leaderOf(name).map(_.isEmpty)) >> body)(_ => unpause(failed)) <*
            eventually("a leader again")(clients.leaderOf(name).map(_.nonEmpty))

      def withReplicaOutOfSync[A](body: IO[A]): IO[A] =
        followers.map(_.head).flatMap: follower =>
          pause(follower).bracket(_ => eventually("out of sync")(clients.inSync(name).map(!_.contains(follower))) >> body)(_ => unpause(follower)) <*
            allInSync

      def appendTimingOutThenLanding(record: Record): IO[CommandLog.Written] =
        // The send goes at once, while the paused replica still counts as in sync, so the leader holds the
        // acknowledgement.
        topic.knowLeader >> followers.map(_.head).flatMap: follower =>
          pause(follower).bracket(_ => topic.sendBriefly(record) <* eventually("the record landed")(endOffset.map(_.value > 0L)))(_ => unpause(follower)) <*
            allInSync

      def appendLostWithItsLeader(record: Record): IO[CommandLog.Written] =
        for
          _ <- topic.knowLeader
          holder <- leader
          others <- followers
          _ <- others.traverse_(pause)
          // A fetch already waiting at the leader would carry the record into a paused replica's socket, and the
          // record would survive. Such a wait lasts `replica.fetch.wait.max.ms`, 500 ms by default.
          _ <- IO.sleep(700.millis)
          written <- topic.sendBriefly(record)
          _ <- pause(holder)
          _ <- others.traverse_(unpause)
          _ <- eventually("another replica leading")(clients.leaderOf(name).map(l => l.nonEmpty && !l.contains(holder)))
          _ <- unpause(holder)
          _ <- allInSync
        yield written
