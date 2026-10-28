package ledger.core

/** A period of the schedule, which names the TigerBeetle cluster that holds the holds of its keys. An opaque
  * `Int`: stored as a primitive.
  */
opaque type Period = Int

object Period:
  def apply(index: Int): Period = index
  extension (p: Period) def index: Int = p
  given CanEqual[Period, Period] = CanEqual.derived

/** The period schedule, fixed in advance like the ID layout (README section 4, one cluster per period). Period 0
  * covers every key before the first switch; period i starts at `switches(i - 1)`. With no
  * switches there is one period, as in stages 1 to 7.
  */
final case class PeriodSchedule(switches: Vector[WallTime]):
  require(switches.zip(switches.drop(1)).forall(_ < _), s"switches must increase: $switches")

  /** The period of a key's creation time: the time comes from the key, never from a clock at the edge. */
  def periodOf(keyTime: WallTime): Period = Period(switches.count(_ <= keyTime))

  def hasStarted(p: Period, now: WallTime): Boolean = p.index == 0 || now >= switches(p.index - 1)

  def hasEnded(p: Period, now: WallTime): Boolean = p.index < switches.size && now >= switches(p.index)

object PeriodSchedule:
  val Single: PeriodSchedule = PeriodSchedule(Vector.empty)
