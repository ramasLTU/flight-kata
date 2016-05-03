package flight.kata.jobs.probabilities

import scala.collection.mutable

class IntervalCounts extends mutable.HashMap[DelayInterval, Int]() {

  def increment(interval: DelayInterval, delta : Int = 1): IntervalCounts = {
    this.put(interval, this.get(interval).getOrElse(0) + delta)
    this
  }

  def merge(other: IntervalCounts): IntervalCounts = {
    other.keySet.foreach( k => increment(k, other.get(k).get) )
    this
  }
}
