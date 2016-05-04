package flight.kata.jobs.probabilities

import flight.kata.jobs.model.Flight
import flight.kata.jobs.{AnalysisJob, AppConfig}
import org.apache.spark.{Logging, SparkContext}

class TopDelayProbabilities(val sparkContext: SparkContext, val takeN: Int = 100, val threshold: Int = 0)
  extends AnalysisJob with AppConfig with Logging {

  def run(dataPath: String): Unit = {

    val fileLines = sparkContext
      .textFile(dataPath)                                   // read data
      .filter(row => row.substring(0, 4) != "Year")         // dirty but effective header removal
      .map(row => Flight(row))                              // map to Flight model

    val delayIntervals = fileLines                          // first, we'll prepare tuples (key, interval)
      .map(flight => ( new AggregateKey(flight), DelayInterval.fromDelay(flight.delay)))

    val combined = delayIntervals
      .combineByKey[IntervalCounts](                        // now let's aggregate by key
      (interval: DelayInterval) => new IntervalCounts().increment(interval),      // build aggregator
      (cnt: IntervalCounts, interval: DelayInterval) => cnt.increment(interval),  // add value to aggregator
      (cnt1: IntervalCounts, cnt2: IntervalCounts) => cnt1.merge(cnt2)            // merge two aggregators
    )
    // serialization bug workaround
    val localThreshold = threshold

    val probabilities = combined
      .filter{ case (key, counts) => counts.sumValues > localThreshold }                   // filter outliers
      .map { case (key, counts) => (key, Probabilities(counts)) }                          // map to probabilities
      .takeOrdered(takeN)(Ordering[Double].reverse.on{ case (key, prob) => prob.over10 })  // take N, ordered desc

    Console.println(s"Top $takeN probabilities to be late more that 10 minutes")
    probabilities.foreach {
      case (key, prob) => Console.println(s"$key $prob")
    }
  }
}






