package flight.kata.jobs

import flight.kata.AppConfig
import flight.kata.schema.FlightSchema
import flight.kata.schema.FlightSchema.ExtensionMethods
import org.apache.spark.{AccumulatorParam, Logging, SparkContext}
;

class TopDelayedLinks(val sparkContext: SparkContext, val takeN: Int = 20)
  extends AnalysisJob with AppConfig with Logging with Serializable {

  def run(): Unit = {

    implicit val param = new TopDelayedLinksStatsAP() // required implicit parameter for accumulator
    val stats = sparkContext.accumulator[TopDelayedLinksStats](TopDelayedLinksStats.empty, "stats")

    val fileLines = sparkContext
      .textFile(appConfig.getString("app.data-dir"))            // read data
      .map(line => line.split(",", -1))                         // split by comma, keep empty columns
      .filter(cells => cells(0) != "Year")                      // dirty, yet effective header removal

    val topDelayed = fileLines
      .map(flight => {                                          // for each flight..
        stats += TopDelayedLinksStats(flight.date)              // ...accumulate stats...
        (flight.route, flight.delay)                            // ...map to (link, delay)
      })
      .filter{ case (route, delay) => delay > 0 }               // filter out on-time flights
      .reduceByKey(_ + _)                                       // sum by link
      .sortBy( { case (route, sum) => sum }, ascending = false) // sort by cumulative delay descending
      .take(takeN)                                              // take N, ordered desc

    topDelayed.foreach {
      case (route, sum) => logInfo(s"$route: $sum total delay minutes")
    }

    logInfo(s"Analyzed ${stats.value.count} links")
    logInfo(s"Earliest date : ${stats.value.minDate} ")
    logInfo(s"Latest date : ${stats.value.maxDate}")
  }
}
