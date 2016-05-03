package flight.kata.jobs.probabilities

import flight.kata.schema.FlightSchema.ExtensionMethods

case class AggregateKey(val origin: String, val destination: String, val dayOfWeek: Int, val timeBlock: Int) {
  def this(flight: Array[String]) = this(flight.origin, flight.destination, flight.dayOfWeek, flight.departureTime / 400)
}
