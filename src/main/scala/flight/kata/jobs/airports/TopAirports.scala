package flight.kata.jobs.airports

import flight.kata.jobs.{AnalysisJob, AppConfig}
import flight.kata.schema.FlightSchema
import flight.kata.schema.FlightSchema.ExtensionMethods
import org.apache.spark.{HashPartitioner, Logging, SparkContext}
;

class TopAirports(val sparkContext: SparkContext, val takeN: Int = 20)
  extends AnalysisJob with AppConfig with Logging {

  def run(): Unit = {
    val fileLines = sparkContext
      .textFile(appConfig.getString("app.data-dir"))  // read data
      .map(line => line.split(",", -1))               // split by comma, keep empty columns
      .filter(cells => cells(0) != "Year")            // dirty, yet effective header removal

    val topAirports = fileLines
      .flatMap(flight => Seq((flight.origin, 1), (flight.destination, 1)))        // map to pairs (airport, 1)
      .reduceByKey(_ + _)                                                         // sum by key, also reduce partitions
      .takeOrdered(takeN)(Ordering[Int].reverse.on{ case (airport, cnt) => cnt }) // take N, ordered desc

    topAirports.foreach {
      case (airport, cnt) => logInfo(s"$airport: $cnt")
    }
  }

}
