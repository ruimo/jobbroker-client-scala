package com.ruimo.jobbroker.client

import java.io.FileInputStream
import java.nio.file.{Path, Paths}
import java.security.KeyStore

import com.rabbitmq.client.ConnectionFactory
import com.typesafe.config.Config
import com.ruimo.scoins.LoanPattern._
import javax.net.ssl.{SSLContext, TrustManagerFactory}

sealed trait SslMode {
  def applyInto(connectionFactory: ConnectionFactory): Unit
}

object SslMode {
  val NoSslCode = "none"
  val ServerSslCode = "server"
  val TrustManagerType = "SunX509"
  val SslCodec = "TLSv1.2"

  case object NoSsl extends SslMode {
    def applyInto(connectionFactory: ConnectionFactory): Unit = {}
  }

  case class ServerSsl(
    trustStorePath: Path, trustStorePassword: String
  ) extends SslMode {
    def applyInto(connectionFactory: ConnectionFactory): Unit = {
      connectionFactory.useSslProtocol(sslContext)
    }

    private def sslContext: SSLContext = {
      val tks = KeyStore.getInstance("JKS")
      using(new FileInputStream(trustStorePath.toFile)) { is =>
        tks.load(is, trustStorePassword.toCharArray)
      }.get
      val tmf = TrustManagerFactory.getInstance(TrustManagerType)
      tmf.init(tks)

      val c = SSLContext.getInstance(SslCodec)
      c.init(null, tmf.getTrustManagers(), null)

      c
    }
  }

  def apply(conf: Config): SslMode = conf.getString("jobbroker.mq.useSsl") match {
    case NoSslCode => NoSsl
    case ServerSslCode => ServerSsl(
      Paths.get(conf.getString("jobbroker.mq.trustStorePath")),
      conf.getString("jobbroker.mq.trustStorePassword")
    )
    case s => throw new IllegalArgumentException("ssl mode '" + s + "' is invalid.")
  }
}


