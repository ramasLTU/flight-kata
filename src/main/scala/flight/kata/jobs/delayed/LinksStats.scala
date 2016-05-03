package flight.kata.jobs.delayed

import org.apache.spark.AccumulatorParam

class LinksStats(val count: Int, val minDate: Int, val maxDate: Int) extends Serializable {

  def this() = this(0, 0, 0)

  def this(date: Int) = this(1, date, date)

  def this(r1: LinksStats, r2: LinksStats) =
    this(r1.count+r2.count, Math.min(r1.minDate, r2.minDate), Math.max(r1.maxDate, r2.maxDate))
}

class LinksStatsAP extends AccumulatorParam[LinksStats] {

  override def zero(initialValue: LinksStats): LinksStats = new LinksStats(0, 0, 0)

  override def addInPlace(r1: LinksStats, r2: LinksStats): LinksStats = {
    if (r1.count == 0) r2 else if (r2.count == 0) r1 else new LinksStats(r1, r2)
  }
}
