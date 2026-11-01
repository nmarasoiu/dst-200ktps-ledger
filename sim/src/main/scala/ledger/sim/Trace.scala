package ledger.sim

import ledger.core.*

/** The order a run took. Its fingerprint is an FNV-1a hash of every event as text, to compare two runs of one seed;
  * events are data, so their text does not depend on the JVM. With `keepLines` it also keeps the text, for reading a
  * failing seed's run.
  */
final class Trace(keepLines: Boolean = false):
  private var hash = 0xcbf29ce484222325L
  private var kept = Vector.empty[String]

  def record(at: HistoryTime, event: Any): Unit =
    val line = s"${at.millis} $event"
    var i = 0
    while i < line.length do
      hash = (hash ^ line.charAt(i)) * 0x100000001b3L
      i += 1
    if keepLines then kept :+= line

  def fingerprint: Long = hash
  def lines: Vector[String] = kept
