package com.ruimo.jobbroker.client

import java.nio.file.Paths

import org.specs2.mutable._

import com.typesafe.config.{Config, ConfigFactory}

class SslModeSpec extends Specification {
  "SslMode" should {
    "Can parse none." in {
      val config: Config = ConfigFactory.parseString(
        """
        jobbroker {
          mq {
            useSsl = "none"
          }
        }
        """
      )
      SslMode(config) === SslMode.NoSsl
    }

    "Can parse server ssl." in {
      val config: Config = ConfigFactory.parseString(
        """
        jobbroker {
          mq {
            useSsl = "server"
            trustStorePath = "/foo/bar"
            trustStorePassword = "mypassword"
          }
        }
        """
      )
      SslMode(config) === SslMode.ServerSsl(Paths.get("/foo/bar"), "mypassword")
    }
  }
}

