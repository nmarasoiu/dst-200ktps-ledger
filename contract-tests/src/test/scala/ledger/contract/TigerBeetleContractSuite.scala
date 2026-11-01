package ledger.contract

import munit.AnyFixture

/** The contract against a real single-node TigerBeetle in Docker, through the real adapter (README section 14,
  * stage 4). Its tests are absent on a laptop without Docker, and in CI that fails instead.
  */
class TigerBeetleContractSuite extends LedgerContract:
  private val run = sys.env.contains("CI") || RealTigerBeetle.dockerAvailable
  private val real = ResourceSuiteLocalFixture("tigerbeetle", RealTigerBeetle.adapter)

  override def munitFixtures: Seq[AnyFixture[?]] = if run then Seq(real) else Seq.empty
  override def munitTests(): Seq[Test] = if run then super.munitTests() else Seq.empty

  def ledger: LedgerUnderTest = RealTigerBeetle.underTest(real())
