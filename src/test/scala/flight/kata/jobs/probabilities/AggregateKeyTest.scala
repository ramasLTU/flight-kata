package flight.kata.jobs.probabilities

import flight.kata.FlightBuilder
import org.scalatest.{Matchers, FunSuite}

import org.mockito.Matchers.{any}

class AggregateKeyTest extends FunSuite with Matchers {

  test("00:00") {
    new AggregateKey(getFlighAt(0)).timeBlock shouldEqual 0
  }

  test("01:12") {
    new AggregateKey(getFlighAt(112)).timeBlock shouldEqual 0
  }

  test("03:59") {
    new AggregateKey(getFlighAt(359)).timeBlock shouldEqual 0
  }

  test("04:00") {
    new AggregateKey(getFlighAt(359)).timeBlock shouldEqual 0
  }

  test("11:35") {
    new AggregateKey(getFlighAt(1135)).timeBlock shouldEqual 2
  }

  test("23:59") {
    new AggregateKey(getFlighAt(2359)).timeBlock shouldEqual 5
  }

  def getFlighAt(time: Int): Array[String] = {
    new FlightBuilder()
      .from(any[String])
      .to(any[String])
      .weekday(any[Int])
      .departureTime(time).row
  }
}
