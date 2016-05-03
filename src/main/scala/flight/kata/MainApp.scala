import flight.kata.jobs.{TopDelayedLinks, TopAirports}
import org.apache.spark.{SparkConf, SparkContext};

object MainApp {

  def main(args: Array[String]): Unit = {

    println("Executing flight-kata")

    val sparkConfig = new SparkConf()
    sparkConfig.setAppName("flight-kata")
    sparkConfig.setMaster("local[2]")

    val sparkContext = new SparkContext(sparkConfig)

    new TopDelayedLinks(sparkContext).run()
  }
}