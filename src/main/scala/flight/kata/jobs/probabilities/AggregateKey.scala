package flight.kata.jobs.probabilities

import flight.kata.jobs.model.Flight

case class AggregateKey(val origin: String, val destination: String, val dayOfWeek: Int, val timeBlock: Int) {
  def this(flight: Flight) = this(flight.origin, flight.destination, flight.dayOfWeek, flight.departureTime / 400)
}
