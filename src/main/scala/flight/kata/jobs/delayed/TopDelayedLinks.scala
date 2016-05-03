package flight.kata.jobs.delayed

import flight.kata.jobs.{AnalysisJob, AppConfig}
import flight.kata.schema.FlightSchema.ExtensionMethods
import org.apache.spark.{HashPartitioner, Logging, SparkContext}
;

class TopDelayedLinks(val sparkContext: SparkContext, val takeN: Int = 20)
  extends AnalysisJob with AppConfig with Logging with Serializable {

  def run(): Unit = {

    implicit val param = new LinksStatsAP() // required implicit parameter for accumulator
    val stats = sparkContext.accumulator[LinksStats](new LinksStats(), "stats")

    val fileLines = sparkContext
      .textFile(appConfig.getString("app.data-dir"))            // read data
      .map(line => line.split(",", -1))                         // split by comma, keep empty columns
      .filter(cells => cells(0) != "Year")                      // dirty, yet effective header removal

    val delayed = fileLines
      .map(flight => {                                          // for each flight..
        stats += new LinksStats(flight.date)                    // ...accumulate stats...
        (flight.route, flight.delay)                            // ...map to (link, delay)
      }).filter{ case (route, delay) => delay > 0 }             // filter out on-time flights

    val topDelayed = delayed
      .reduceByKey(new HashPartitioner(10), _ + _)                              // sum by link, also reduce partitions
      .takeOrdered(takeN)(Ordering[Int].reverse.on{ case (route, sum) => sum }) // take N, ordered desc

    topDelayed.foreach {
      case (route, sum) => logInfo(s"$route: $sum total delay minutes")
    }

    logInfo(s"Analyzed ${stats.value.count} links")
    logInfo(s"Earliest date : ${stats.value.minDate} ")
    logInfo(s"Latest date : ${stats.value.maxDate}")
  }
}
