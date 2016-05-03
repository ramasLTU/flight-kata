package flight.kata.integ

import flight.kata.AppConfig
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FunSuite

class CheckHeadersInteg extends FunSuite with AppConfig {

  test("inspect all headers") {

    val sparkConfig = new SparkConf()
    sparkConfig.setAppName("flight-kata")
    sparkConfig.setMaster("local[2]")

    val sparkContext = new SparkContext(sparkConfig)

    val headers = sparkContext
      .textFile("../all-data")
      .map(_.split(",", -1))
      .filter(cells => cells(0) == "Year")
      .map(_.mkString(","))
      .collect()

    headers.foreach(println)
  }

}
