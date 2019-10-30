package com.ruimo.jobbroker.client

import java.io.{InputStream, PrintWriter, StringWriter}
import java.sql.{Connection, DriverManager}
import java.time.Instant

import com.rabbitmq.client.{Channel, ConnectionFactory}
import com.rabbitmq.client.{Connection => MqConnection}
import com.ruimo.jobbroker.JobId
import com.ruimo.jobbroker.dao.{AccountId, ApplicationId, JobStatus, Request}
import com.ruimo.jobbroker.queue.{JobQueue, WaitingJobHandle}
import com.ruimo.scoins.LoanPattern._
import com.ruimo.scoins.ResourceWrapper
import com.typesafe.config.{Config, ConfigFactory}
import com.ruimo.scoins.Scoping._
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success}

class ClientContext(
  dbConnFactory: () => Connection,
  mqConnFactory: () => MqConnection,
) {
  def withClient[T](
    f: Client => T
  ): T = {
    ClientContext.Logger.info("Start ClientContext.withClient().")

    try {
      using(new ResourceWrapper[Connection](dbConnFactory)) { conn =>
        using(new ResourceWrapper[MqConnection](mqConnFactory)) { mqConn =>
          f(new Client(conn, mqConn))
        }.get
      }.get
    } finally {
      ClientContext.Logger.info("End ClientContext.withClient().")
    }
  }

  def submitJobWithBytes(
    accountId: AccountId, applicationId: ApplicationId, in: Array[Byte], now: Instant = Instant.now()
  ): Request = withClient(_.submitJobWithBytes(accountId, applicationId, in, now))

  def submitJobWithStream(
    accountId: AccountId, applicationId: ApplicationId, is: InputStream, now: Instant = Instant.now()
  ): Request = withClient(_.submitJobWithStream(accountId, applicationId, is, now))

  def retrieveJobWithBytes(
    onJobObtained: (Request, Array[Byte]) => Unit,
    onCancel: () => Unit,
    onError: (JobId, Throwable) => Unit,
    now: Instant = Instant.now()
  ): WaitingJobHandle = withClient(_.retrieveJobWithBytes(onJobObtained, onCancel, onError, now))

  def retrieveJobWithStream(
    onJobObtained: (Request, InputStream) => Unit,
    onCancel: () => Unit,
    onError: (JobId, Throwable) => Unit,
    now: Instant = Instant.now()
  ): WaitingJobHandle = withClient(_.retrieveJobWithStream(onJobObtained, onCancel, onError, now))

  def cancelJobWaiting(
    handle: WaitingJobHandle
  ) {
    withClient(_.cancelJobWaiting(handle))
  }
}

trait ClientContextFactory {
  def createInstance(): ClientContext
}

object ClientContext extends ClientContextFactory {
  val conf: Config = ConfigFactory.load()
  val Logger: Logger = LoggerFactory.getLogger(getClass)

  def apply(
    dbUrl: String = conf.getString("jobbroker.db.url"),
    dbUser: String = conf.getString("jobbroker.db.user"),
    dbPassword: String = conf.getString("jobbroker.db.password"),
    mqHost: String = conf.getString("jobbroker.mq.host"),
    mqPort: Int = conf.getInt("jobbroker.mq.port"),
    mqUser: String = conf.getString("jobbroker.mq.user"),
    mqPassword: String = conf.getString("jobbroker.mq.password")
  ): ClientContext = {
    Logger.info("dbUrl = '" + dbUrl + "'")
    Logger.info("dbUser = '" + dbUser + "'")
    Logger.info("mqHost = '" + mqHost + "'")
    Logger.info("mqPort = '" + mqPort + "'")
    Logger.info("mqUser = '" + mqUser + "'")

    val connectionFactory = new ConnectionFactory()
    connectionFactory.setUsername(mqUser)
    connectionFactory.setPassword(mqPassword)
    connectionFactory.setHost(mqHost)
    connectionFactory.setPort(mqPort)

    new ClientContext(
      () => DriverManager.getConnection(dbUrl, dbUser, dbPassword),
      () => connectionFactory.newConnection()
    )
  }

  override def createInstance(): ClientContext = ClientContext()
}
