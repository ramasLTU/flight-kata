package flight.kata.jobs.delayed

import flight.kata.jobs.model.Flight
import flight.kata.jobs.{AnalysisJob, AppConfig}
import org.apache.spark.{Logging, SparkContext}

class TopDelayedLinks(val sparkContext: SparkContext, val takeN: Int = 20)
  extends AnalysisJob with AppConfig with Logging with Serializable {

  def run(dataPath: String): Unit = {

    implicit val param = new LinksStatsAP() // required implicit parameter for accumulator
    val stats = sparkContext.accumulator[LinksStats](new LinksStats(), "stats")

    val fileLines = sparkContext
      .textFile(dataPath)                                       // read data
      .filter(row => row.substring(0, 4) != "Year")             // dirty but effective header removal
      .map(row => Flight(row))                                  // map to Flight model

    val delayed = fileLines
      .map(flight => {                                          // for each flight..
        stats += new LinksStats(flight.date)                    // ...accumulate stats...
        (flight.route, flight.delay)                            // ...map to (link, delay)
      }).filter{ case (route, delay) => delay > 0 }             // filter out on-time flights

    val topDelayed = delayed
      .reduceByKey(_ + _)                                       // sum by link, also reduce partitions
      .takeOrdered(takeN)(Ordering[Int].reverse.on{ case (route, sum) => sum }) // take N, ordered desc

    Console.println(s"Top $takeN delayed links")
    topDelayed.foreach {
      case (route, sum) => Console.println(s"$route: $sum total delay minutes")
    }

    Console.println(s"Analyzed ${stats.value.count} links")
    Console.println(s"Earliest date : ${stats.value.minDate} ")
    Console.println(s"Latest date : ${stats.value.maxDate}")
  }
}
