package flight.kata.jobs.probabilities

case class Probabilities(totalEvents: Int, onTime: Double, under10: Double, over10: Double) {
  override def toString: String = {
    f"Probabilities($onTime%1.1f, $under10%1.1f, $over10%1.1f) based on $totalEvents event(s)"
  }
}

object Probabilities {
  def apply(counts: IntervalCounts) = {
    counts.sumValues match {
      case 0 => new Probabilities(0, 0, 0, 0)
      case totalCount => new Probabilities(
        totalCount,
        100.0 * counts.get(DelayInterval.OnTime) / totalCount,
        100.0 * counts.get(DelayInterval.Under10) / totalCount,
        100.0 * counts.get(DelayInterval.Over10) / totalCount
      )
    }
  }
}
