package flight.kata

import flight.kata.jobs.airports.TopAirports
import flight.kata.jobs.delayed.TopDelayedLinks
import flight.kata.jobs.probabilities.TopDelayProbabilities

import org.apache.spark.{SparkContext, SparkConf}

class RunTopAirports extends Runner {
  def main(args: Array[String]): Unit = {
    new TopAirports(buildSparkContext("TopAirports")).run(getDataPath(args))
  }
}

class RunTopDelayedLinks extends Runner {
  def main(args: Array[String]): Unit = {
    new TopDelayedLinks(buildSparkContext("TopDelayedLinks")).run(getDataPath(args))
  }
}

class RunTopDelayProbabilities extends Runner {
  def main(args: Array[String]): Unit = {
    new TopDelayProbabilities(buildSparkContext("TopDelayProbabilities")).run(getDataPath(args))
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

  def getDataPath(args: Array[String]): String = {
    if (args.length != 1) {
      System.err.println("Usage: <data path>")
      System.exit(1)
    }
    args(0)
  }

}
