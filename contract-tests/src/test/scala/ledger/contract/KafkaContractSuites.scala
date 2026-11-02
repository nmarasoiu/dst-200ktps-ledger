package ledger.contract

import munit.AnyFixture

/** The contract against one real broker, under each group protocol. Absent on a laptop without Docker, and in CI that
  * fails instead.
  */
abstract class KafkaContractSuite(protocol: RealKafka.Protocol, throughAdapter: Boolean = false) extends LogContract:
  private val run = sys.env.contains("CI") || RealTigerBeetle.dockerAvailable
  private val real = ResourceSuiteLocalFixture("kafka", RealKafka.singleBroker(protocol, throughAdapter))

  override def munitFixtures: Seq[AnyFixture[?]] = if run then Seq(real) else Seq.empty
  override def munitTests(): Seq[Test] = if run then super.munitTests() else Seq.empty

  def log: LogUnderTest = real()

class KafkaClassicProtocolContractSuite extends KafkaContractSuite(RealKafka.Protocol.Classic)
class KafkaConsumerProtocolContractSuite extends KafkaContractSuite(RealKafka.Protocol.Consumer)

/** The same contract through the shell's fs2-kafka adapter, on the Kafka client this build pins. The adapter polls on its
  * own fiber, so no member can go quiet here: a paused process is how a real zombie arises, and the end-to-end suite
  * pauses one.
  */
abstract class Fs2KafkaContractSuite(protocol: RealKafka.Protocol) extends KafkaContractSuite(protocol, throughAdapter = true):
  override def munitTests(): Seq[Test] = super.munitTests().filterNot(_.name.startsWith("a member that goes quiet"))

class Fs2KafkaClassicProtocolContractSuite extends Fs2KafkaContractSuite(RealKafka.Protocol.Classic)
class Fs2KafkaConsumerProtocolContractSuite extends Fs2KafkaContractSuite(RealKafka.Protocol.Consumer)

/** The faults against three real brokers. */
abstract class KafkaFaultsSuite(throughAdapter: Boolean) extends LogFaultsContract:
  private val run = sys.env.contains("CI") || RealTigerBeetle.dockerAvailable
  private val real = ResourceSuiteLocalFixture("kafka-cluster", RealKafka.threeBrokers(throughAdapter))

  override def munitFixtures: Seq[AnyFixture[?]] = if run then Seq(real) else Seq.empty
  override def munitTests(): Seq[Test] = if run then super.munitTests() else Seq.empty

  def log: FaultyLogUnderTest = real()

class KafkaFaultsContractSuite extends KafkaFaultsSuite(throughAdapter = false)
class Fs2KafkaFaultsContractSuite extends KafkaFaultsSuite(throughAdapter = true)
