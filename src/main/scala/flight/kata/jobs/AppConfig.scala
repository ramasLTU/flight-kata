package flight.kata.jobs

import com.typesafe.config.{Config, ConfigFactory}

trait AppConfig {
  val appConfig: Config = ConfigFactory.load()
}
