package flight.kata.jobs

import flight.kata.AppConfig
import flight.kata.schema.FlightSchema
import org.apache.spark.{Logging, SparkContext}

import FlightSchema.ExtensionMethods;

class TopAirports(val sparkContext: SparkContext, val takeN: Int = 20)
  extends AnalysisJob with AppConfig with Logging {

  def run(): Unit = {
    val fileLines = sparkContext
      .textFile(appConfig.getString("app.data-dir"))  // read data
      .map(line => line.split(",", -1))               // split by comma, keep empty columns
      .filter(cells => cells(0) != "Year")            // dirty, yet effective header removal

    val topAirports = fileLines
      .flatMap(flight => Seq((flight.origin, 1), (flight.destination, 1)))  // map to pairs (airport, 1)
      .reduceByKey(_ + _)                                                   // sum by key
      .sortBy( { case (airport, cnt) => cnt }, ascending = false)           // sort by count descending
      .take(takeN)                                                          // take N, ordered desc

    topAirports.foreach {
      case (airport, cnt) => logInfo(s"$airport: $cnt")
    }
  }

}
