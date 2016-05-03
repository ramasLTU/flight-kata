package flight.kata.jobs.probabilities

import org.scalatest.{Matchers, FunSuite}

class IntervalCountsTest extends FunSuite with Matchers {

  test("empty") {
    new IntervalCounts().size shouldEqual 0
  }

  test("increment") {
    val counts = new IntervalCounts().increment(DelayInterval.OnTime)
    counts.size shouldEqual 1
    counts.getOrElse(DelayInterval.OnTime, 0) shouldEqual 1
  }

  test("repeated increments") {
    val counts = new IntervalCounts()
      .increment(DelayInterval.OnTime)
      .increment(DelayInterval.OnTime)
      .increment(DelayInterval.OnTime)
    counts.size shouldEqual 1
    counts.getOrElse(DelayInterval.OnTime, 0) shouldEqual 3
  }

  test("mixed increments") {
    val counts = new IntervalCounts()
      .increment(DelayInterval.Under10)
      .increment(DelayInterval.Over10)
      .increment(DelayInterval.Under10)
    counts.size shouldEqual 2
    counts.getOrElse(DelayInterval.Under10, 0) shouldEqual 2
    counts.getOrElse(DelayInterval.Over10, 0) shouldEqual 1
  }

  test("bigger increment") {
    val counts = new IntervalCounts()
      .increment(DelayInterval.OnTime, 3)
      .increment(DelayInterval.OnTime, 2)
    counts.getOrElse(DelayInterval.OnTime, 0) shouldEqual 5
  }

  test("repeated bigger increments") {
    val counts = new IntervalCounts().increment(DelayInterval.OnTime, 3)
    counts.getOrElse(DelayInterval.OnTime, 0) shouldEqual 3
  }

  test("merge") {
    val cnt1 = new IntervalCounts()
      .increment(DelayInterval.Under10, 5)
      .increment(DelayInterval.OnTime, 1)

    val cnt2 = new IntervalCounts()
      .increment(DelayInterval.Under10, 2)
      .increment(DelayInterval.Over10, 3)

    val merged = cnt1.merge(cnt2)

    merged.size shouldEqual 3
    merged.getOrElse(DelayInterval.OnTime, 0) shouldEqual 1
    merged.getOrElse(DelayInterval.Under10, 0) shouldEqual 7
    merged.getOrElse(DelayInterval.Over10, 0) shouldEqual 3
  }

}
