package flight.kata.jobs

trait AnalysisJob {
  def run(dataPath: String): Unit
}
