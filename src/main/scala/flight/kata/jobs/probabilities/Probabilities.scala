package flight.kata.jobs.probabilities

case class Probabilities(onTime: Double, under10: Double, over10: Double) {
  override def toString: String = {
    f"Probabilities($onTime%1.1f, $under10%1.1f, $over10%1.1f)"
  }
}

object Probabilities {
  def apply(counts: IntervalCounts) = {
    counts.values.sum match {
      case 0 => new Probabilities(0, 0, 0)
      case totalCount => new Probabilities(
        100.0 * counts.getOrElse(DelayInterval.OnTime, 0) / totalCount,
        100.0 * counts.getOrElse(DelayInterval.Under10, 0) / totalCount,
        100.0 * counts.getOrElse(DelayInterval.Over10, 0) / totalCount
      )
    }
  }
}
