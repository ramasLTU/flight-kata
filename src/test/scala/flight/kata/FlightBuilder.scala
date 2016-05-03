package flight.kata

import com.typesafe.config.ConfigFactory
import flight.kata.jobs.AppConfig

import scala.collection.JavaConverters._

class FlightBuilder extends AppConfig {

  val indexOf: Map[String, Int] = ConfigFactory.load().getStringList("app.schema").asScala
    .zipWithIndex
    .map { case (name, index) => name -> index}
    .toMap

  val row = new Array[String](indexOf.size)

  def year(value: Int) = set("Year", value)
  def month(value: Int) = set("Month", value)
  def day(value: Int) = set("DayofMonth", value)
  def weekday(value: Int) = set("DayOfWeek", value)
  def departureTime(value: Int) = set("DepTime", value)

  def from(value: String) = set("Origin", value)
  def to(value: String) = set("Dest", value)

  private def set(name: String, value:Int): FlightBuilder = {
    set(name, value.toString)
  }

  private def set(name: String, value: String): FlightBuilder = {
    row(indexOf(name)) = value
    this
  }
}
