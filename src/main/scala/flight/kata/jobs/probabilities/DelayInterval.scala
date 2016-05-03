package flight.kata.jobs.probabilities

sealed trait DelayInterval extends Serializable

object DelayInterval {

  case object OnTime extends DelayInterval
  case object Under10 extends DelayInterval
  case object Over10 extends DelayInterval

  def fromDelay(delay: Int): DelayInterval = {
    if (delay <= 0)
      DelayInterval.OnTime
    else if (delay < 10)
      DelayInterval.Under10
    else
      DelayInterval.Over10
  }
}
