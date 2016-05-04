package flight.kata.jobs.airports

import flight.kata.jobs.model.Flight
import flight.kata.jobs.{AnalysisJob, AppConfig}
import org.apache.spark.{Logging, SparkContext}

class TopAirports(val sparkContext: SparkContext, val takeN: Int = 20)
  extends AnalysisJob with AppConfig with Logging {

  def run(): Unit = {
    val fileLines = sparkContext
      .textFile(appConfig.getString("app.data-dir"))            // read data
      .filter(row => row.substring(0, 4) != "Year")             // dirty but effective header removal
      .map(row => Flight(row))                                  // map to Flight model

    val topAirports = fileLines
      .flatMap(flight => Seq((flight.origin, 1), (flight.destination, 1)))        // map to pairs (airport, 1)
      .reduceByKey(_ + _)                                                         // sum by key, also reduce partitions
      .takeOrdered(takeN)(Ordering[Int].reverse.on{ case (airport, cnt) => cnt }) // take N, ordered desc

    topAirports.foreach {
      case (airport, cnt) => Console.println(s"$airport: $cnt")
    }
  }

}
