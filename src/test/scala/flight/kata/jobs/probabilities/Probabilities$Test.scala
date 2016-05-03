package flight.kata.jobs.probabilities

import org.scalatest.{Matchers, FunSuite}

class Probabilities$Test extends FunSuite with Matchers {

  test("empty") {
    val probs = Probabilities(new IntervalCounts())
    probs.onTime shouldEqual 0
    probs.under10 shouldEqual 0
    probs.over10 shouldEqual 0
  }

  test("distribution") {

    val counts = new IntervalCounts()
      .increment(DelayInterval.OnTime, 2)
      .increment(DelayInterval.Under10, 8)

    val probs = Probabilities(counts)
    probs.onTime shouldEqual 20.0
    probs.under10 shouldEqual 80.0
    probs.over10 shouldEqual 0.0
  }

}
