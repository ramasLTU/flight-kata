package flight.kata

import flight.kata.jobs.airports.TopAirports
import flight.kata.jobs.delayed.TopDelayedLinks
import flight.kata.jobs.probabilities.TopDelayProbabilities

import org.apache.spark.{SparkContext, SparkConf}

object LocalRunTopAirports extends LocalRunner {
  def main(args: Array[String]): Unit = {
    new TopAirports(buildSparkContext("TopAirports")).run("data")
  }
}

object LocalRunTopDelayedLinks extends LocalRunner {
  def main(args: Array[String]): Unit = {
    new TopDelayedLinks(buildSparkContext("TopDelayedLinks")).run("data")
  }
}

object LocalRunTopDelayProbabilities extends LocalRunner {
  def main(args: Array[String]): Unit = {
    new TopDelayProbabilities(buildSparkContext("TopDelayProbabilities")).run("data")
  }
}

object LocalRunTopDelayProbabilitiesWithThreshold extends LocalRunner {
  def main(args: Array[String]): Unit = {
    new TopDelayProbabilities(buildSparkContext("TopDelayProbabilities"), threshold = 100).run("data")
  }
}

abstract class LocalRunner {
  def buildSparkContext(application: String) : SparkContext = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[2]")
    sparkConf.setAppName(application)
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    new SparkContext(sparkConf)
  }
}
