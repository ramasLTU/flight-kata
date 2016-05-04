package flight.kata.jobs.probabilities

import flight.kata.jobs.model.Flight
import org.mockito.Matchers.any
import org.scalatest.{FunSuite, Matchers}

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

  def getFlighAt(time: Int): Flight = {
    new Flight(any[Int], any[Int], any[String], any[String], time, any[Int])
  }
}
