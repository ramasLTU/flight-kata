package flight.kata.jobs.probabilities

import flight.kata.jobs.{AnalysisJob, AppConfig}
import flight.kata.schema.FlightSchema.ExtensionMethods

import org.apache.spark.{Logging, SparkContext}

class TopDelayProbabilities(val sparkContext: SparkContext, val takeN: Int = 100)
  extends AnalysisJob with AppConfig with Logging {

  def run(): Unit = {

    val fileLines = sparkContext
      .textFile(appConfig.getString("app.data-dir")) // read data
      .map(line => line.split(",", -1))                     // split by comma, keep empty columns
      .filter(cells => cells(0) != "Year")                  // dirty, yet effective header removal

    val delayIntervals = fileLines                          // first, we'll prepare tuples (key, interval)
      .map(flight => ( new AggregateKey(flight), DelayInterval.fromDelay(flight.delay)))

    val combined = delayIntervals
      .combineByKey[IntervalCounts](                        // now let's aggregate by key
      (interval: DelayInterval) => new IntervalCounts().increment(interval),      // build aggregator
      (cnt: IntervalCounts, interval: DelayInterval) => cnt.increment(interval),  // add value to aggregator
      (cnt1: IntervalCounts, cnt2: IntervalCounts) => cnt1.merge(cnt2)            // merge two aggregators
    ) // no point in repartition here, too many unique keys...

    val probabilities = combined
      .map { case (key, counts) => (key, Probabilities(counts)) }                          // map to probabilities
      .takeOrdered(takeN)(Ordering[Double].reverse.on{ case (key, prob) => prob.over10 })  // take N, ordered desc

    probabilities.foreach {
      case (key, prob) => logInfo(s"$key $prob")
    }
  }
}






