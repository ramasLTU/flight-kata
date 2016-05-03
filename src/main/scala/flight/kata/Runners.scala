package flight.kata

import flight.kata.jobs.airports.TopAirports
import flight.kata.jobs.delayed.TopDelayedLinks
import flight.kata.jobs.probabilities.TopDelayProbabilities

import org.apache.spark.{SparkContext, SparkConf}

object RunTopAirports extends Runner {
  def main(args: Array[String]): Unit = {
    new TopAirports(buildSparkContext("TopAirports")).run()
  }
}

object RunTopDelayedLinks extends Runner {
  def main(args: Array[String]): Unit = {
    new TopDelayedLinks(buildSparkContext("TopDelayedLinks")).run()
  }
}

object RunTopDelayProbabilities extends Runner {
  def main(args: Array[String]): Unit = {
    new TopDelayProbabilities(buildSparkContext("TopDelayProbabilities")).run()
    Console.readLine();
  }
}

abstract class Runner {
  def buildSparkContext(application: String) : SparkContext = {

    val sparkConf = new SparkConf()

    sparkConf.setAppName(application)

    // change default serializer to Krio
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    new SparkContext(sparkConf)
  }
}
