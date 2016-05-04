package flight.kata.jobs.model

import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters._

case class Flight(date: Int, dayOfWeek: Int, origin: String, destination: String, departureTime: Int, delay: Int) {
  val route = s"$origin - $destination"
}

object Flight {

  val na = "NA"

  val indexOf: Map[String, Int] = ConfigFactory.load().getStringList("app.schema").asScala
    .zipWithIndex
    .map { case (name, index) => name -> index }
    .toMap

  def apply(row: String): Flight = {
    Flight(row.split(","))
  }

  def apply(row: Array[String]): Flight = new Flight(
    getDate(row),
    getValueInt(row, "DayOfWeek"),
    getValue(row, "Origin"),
    getValue(row, "Dest"),
    getValueInt(row, "DepTime"),
    getValueInt(row, "ArrDelay")
  )

  private def notAvailable(value: String): Boolean = {
    na.equals(value)
  }

  private def getValue(row: Array[String], column: String): String = {
    val optionalIdx = indexOf.get(column)
    if (optionalIdx.isEmpty) throw new RuntimeException(s"Unknown column: $column")
    val idx = optionalIdx.get
    if (idx < 0 || idx > row.length)
      throw new RuntimeException(s"The column index for $column was out of bounds, idx: $idx. Record: ${row.mkString("\t")}")
    row(idx)
  }

  private def getValueInt(row: Array[String], column: String): Int = {
    val txt = getValue(row, column)
    if (na.equals(txt)) 0 else txt.toInt
  }

  private def getDate(row: Array[String]): Int = {
    getValue(row, "Year").toInt * 10000 + getValue(row, "Month").toInt * 100 + getValue(row, "DayofMonth").toInt
  }
}
