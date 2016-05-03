package flight.kata.jobs.probabilities

import scala.collection.mutable.HashMap

class IntervalCounts extends Serializable {

  private val map = new HashMap[DelayInterval, Int]

  def size = map.size

  def sumValues = map.values.sum

  def get(interval: DelayInterval): Int = map.getOrElse(interval, 0)

  def increment(interval: DelayInterval, delta : Int = 1): IntervalCounts = {
    map.put(interval, get(interval) + delta)
    this
  }

  def merge(other: IntervalCounts): IntervalCounts = {
    other.map.keySet.foreach( k => increment(k, other.get(k)) )
    this
  }
}
