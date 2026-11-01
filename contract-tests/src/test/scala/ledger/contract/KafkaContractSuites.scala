package ledger.contract

import munit.AnyFixture

/** The contract against one real broker, under each group protocol. Absent on a laptop without Docker, and in CI that
  * fails instead.
  */
abstract class KafkaContractSuite(protocol: RealKafka.Protocol) extends LogContract:
  private val run = sys.env.contains("CI") || RealTigerBeetle.dockerAvailable
  private val real = ResourceSuiteLocalFixture("kafka", RealKafka.singleBroker(protocol))

  override def munitFixtures: Seq[AnyFixture[?]] = if run then Seq(real) else Seq.empty
  override def munitTests(): Seq[Test] = if run then super.munitTests() else Seq.empty

  def log: LogUnderTest = real()

class KafkaClassicProtocolContractSuite extends KafkaContractSuite(RealKafka.Protocol.Classic)
class KafkaConsumerProtocolContractSuite extends KafkaContractSuite(RealKafka.Protocol.Consumer)

/** The faults against three real brokers. */
class KafkaFaultsContractSuite extends LogFaultsContract:
  private val run = sys.env.contains("CI") || RealTigerBeetle.dockerAvailable
  private val real = ResourceSuiteLocalFixture("kafka-cluster", RealKafka.threeBrokers)

  override def munitFixtures: Seq[AnyFixture[?]] = if run then Seq(real) else Seq.empty
  override def munitTests(): Seq[Test] = if run then super.munitTests() else Seq.empty

  def log: FaultyLogUnderTest = real()
