package ledger.contract

import cats.effect.{IO, Resource}
import java.net.InetAddress
import ledger.core.*
import ledger.shell.TigerBeetle
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*

/** A single-node TigerBeetle in Docker, reached through the real adapter. `--development` allows a data file on a
  * file system without direct I/O, and a small grid cache lets it fit on a laptop. io_uring is why the container
  * needs seccomp unconfined.
  */
object RealTigerBeetle:
  val Image = "ghcr.io/tigerbeetle/tigerbeetle:0.17.9"
  private val Port = 3000
  val Cluster: Period = Period(0)

  /** On a laptop without Docker the real half skips. In CI it must fail instead, or the suite could go silently
    * unrun.
    */
  def dockerAvailable: Boolean = DockerClientFactory.instance().isDockerAvailable

  private def startCommand: String =
    val file = "/tmp/0_0.tigerbeetle"
    s"/tigerbeetle format --cluster=0 --replica=0 --replica-count=1 --development $file && " +
      s"exec /tigerbeetle start --addresses=0.0.0.0:$Port --development --cache-grid=128MiB $file"

  private def container: Resource[IO, GenericContainer[?]] =
    Resource.make(
      IO.blocking:
        val c = new GenericContainer(DockerImageName.parse(Image))
        c.withExposedPorts(Port)
        c.withCreateContainerCmdModifier: cmd =>
          cmd.withEntrypoint("sh", "-c", startCommand)
          cmd.getHostConfig.withSecurityOpts(List("seccomp=unconfined").asJava)
          ()
        c.waitingFor(Wait.forLogMessage(".*listening on.*\\n", 1))
        c.start()
        c,
    )(c => IO.blocking(c.stop()))

  /** The address of a running server. The client takes addresses, not host names. */
  def reachable: Resource[IO, String] =
    container.map(c => s"${InetAddress.getByName(c.getHost).getHostAddress}:${c.getMappedPort(Port)}")

  def adapterOf(address: String): Resource[IO, TigerBeetle] =
    TigerBeetle.resource(Map(Cluster -> TigerBeetle.Cluster(U128.Zero, Vector(address))))

  def adapter: Resource[IO, TigerBeetle] = reachable.flatMap(adapterOf)

  def underTest(tb: TigerBeetle): LedgerUnderTest = new LedgerUnderTest:
    def createAccounts(accounts: Vector[Ledger.NewAccount]): IO[Unit] = tb.createAccounts(Ledger.CreateAccounts(Cluster, accounts))
    def submit(transfers: Vector[Transfer]): IO[Ledger.Result] = tb.submit(Ledger.Submit(Cluster, transfers))
    def scan(after: Long, limit: Int): IO[Vector[LedgerTransfer]] = tb.scan(Ledger.Scan(Cluster, after, limit))
    def lookupAccounts(ids: Vector[AccountId]): IO[Vector[Ledger.Balances]] = tb.lookupAccounts(Ledger.LookupAccounts(Cluster, ids))
    def closeAccount(id: AccountId, other: AccountId, ledger: Int, code: Int): IO[Unit] =
      tb.closeAccount(Ledger.CloseAccount(Cluster, id, other, ledger, code))
    def passTime(d: FiniteDuration): IO[Unit] = IO.sleep(d)
    def elapsed: IO[FiniteDuration] = IO.monotonic
