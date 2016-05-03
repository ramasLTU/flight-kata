package flight.kata.jobs

import org.apache.spark.AccumulatorParam

class TopDelayedLinksStats(val count: Int, val minDate: Int, val maxDate: Int) extends Serializable

object TopDelayedLinksStats {

  def empty: TopDelayedLinksStats = new TopDelayedLinksStats(0, 0, 0)

  def apply(date: Int): TopDelayedLinksStats = new TopDelayedLinksStats(1, date, date)

  def apply(r1: TopDelayedLinksStats, r2: TopDelayedLinksStats): TopDelayedLinksStats = {
    new TopDelayedLinksStats(r1.count+r2.count, Math.min(r1.minDate, r2.minDate), Math.max(r1.maxDate, r2.maxDate))
  }
}

class TopDelayedLinksStatsAP extends AccumulatorParam[TopDelayedLinksStats] {

  override def zero(initialValue: TopDelayedLinksStats): TopDelayedLinksStats = new TopDelayedLinksStats(0, 0, 0)

  override def addInPlace(r1: TopDelayedLinksStats, r2: TopDelayedLinksStats): TopDelayedLinksStats = {
    if (r1.count == 0) r2 else if (r2.count == 0) r1 else TopDelayedLinksStats(r1, r2)
  }
}
