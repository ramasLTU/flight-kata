package flight.kata.schema

import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._

object FlightSchema {

  val na = "NA"

  val indexOf: Map[String, Int] = ConfigFactory.load().getStringList("app.schema").asScala
    .zipWithIndex
    .map { case (name, index) => name -> index}
    .toMap

  implicit class ExtensionMethods(val row: Array[String]) {

    def date: Int = {
      getValue("Year").toInt * 10000 + getValue("Month").toInt * 100 + getValue("DayofMonth").toInt
    }

    def origin: String = {
      getValue("Origin")
    }

    def destination: String = {
      getValue("Dest")
    }

    def route: String = {
      s"$origin - $destination"
    }

    def delay: Int = {
      val txt = getValue("ArrDelay")
      if (notAvailable(txt)) 0 else txt.toInt
    }

    private def notAvailable(value: String): Boolean = {
      na.equals(value)
    }

    private def getValue(column: String): String = {
      val optionalIdx = indexOf.get(column)
      if (optionalIdx.isEmpty) throw new RuntimeException(s"Unknown column: $column")
      val idx = optionalIdx.get
      if (idx < 0 || idx > row.row.length)
        throw new RuntimeException(s"The column index for $column was out of bounds, idx: $idx. Record: ${row.row.mkString("\t")}")
      row(idx)
    }
  }

}
