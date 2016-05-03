package flight.kata

import com.typesafe.config.{Config, ConfigFactory}

trait AppConfig {
  val appConfig: Config = ConfigFactory.load()
}
