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

class Client(conn: ResourceWrapper[Connection], mqConn: ResourceWrapper[MqConnection]) {
  val logger: Logger = LoggerFactory.getLogger(getClass)
  lazy val channel = mqConn().createChannel()
  lazy val jobQueue: JobQueue = new JobQueue(channel)

  def submitJobWithBytes(
    accountId: AccountId, applicationId: ApplicationId, in: Array[Byte], now: Instant = Instant.now()
  ): Request = {
    val req = Request.submitJobWithBytes(accountId, applicationId, in, now)(conn())
    jobQueue.submitJob(req.id)
    req
  }

  def submitJobWithStream(
    accountId: AccountId, applicationId: ApplicationId, in: InputStream, now: Instant = Instant.now()
  ): Request = {
    val req = Request.submitJobWithStream(accountId, applicationId, in, now)(conn())
    jobQueue.submitJob(req.id)
    req
  }

  def retrieveJob[T](
    onJobObtained: (Request, T) => Unit,
    onCancel: () => Unit,
    onError: (JobId, Throwable) => Unit,
    retrieveJob: (JobId, Connection) => (Request, T)
  ): WaitingJobHandle = {
    def perform(jobId: JobId) {
      val (request: Request, job: T) = retrieveJob(jobId, conn())
      onJobObtained(request, job)
    }

    def error(jobId: JobId, t: Throwable) {
      try {
        Request.storeJobResultWithBytes(
          jobId,
          Client.stackTraceToString(t).getBytes("utf-8"),
          jobStatus = JobStatus.JobSystemError
        )(conn())
      } catch {
        case t: Throwable =>
          logger.error("Cannot save error.", t)
      }
      onError(jobId, t)
    }

    jobQueue.waitJob(perform, onCancel, error)
  }

  def retrieveJobWithBytes(
    onJobObtained: (Request, Array[Byte]) => Unit,
    onCancel: () => Unit,
    onError: (JobId, Throwable) => Unit,
    now: Instant = Instant.now()
  ): WaitingJobHandle = retrieveJob[Array[Byte]](
    onJobObtained, onCancel, onError,
    (jobId, c) => Request.retrieveJobWithBytes(jobId, now)(c)
  )

  def retrieveJobWithStream(
    onJobObtained: (Request, InputStream) => Unit,
    onCancel: () => Unit,
    onError: (JobId, Throwable) => Unit,
    now: Instant = Instant.now()
  ): WaitingJobHandle = retrieveJob[InputStream](
    onJobObtained, onCancel, onError,
    (jobId, c) => Request.retrieveJobWithStream(jobId, now)(c)
  )

  def cancelJobWaiting(
    handle: WaitingJobHandle
  ) {
    jobQueue.cancelJobWaiting(handle)
  }
}

object Client {
  def stackTraceToString(t: Throwable): String = using(new StringWriter()) { sw =>
    using(new PrintWriter(sw)) { pw =>
      t.printStackTrace(pw)
    }.get
    sw.toString
  }.get
}

class ClientContext(
  dbConnFactory: () => Connection,
  mqConnFactory: () => MqConnection,
) {
  def withClient[T](
    f: Client => T
  ): T =
    using(new ResourceWrapper[Connection](dbConnFactory)) { conn =>
      using(new ResourceWrapper[MqConnection](mqConnFactory)) { mqConn =>
        f(new Client(conn, mqConn))
      }.get
    }.get

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

object ClientContext {
  val conf: Config = ConfigFactory.load()

  def apply(
    dbUrl: String = conf.getString("jobbroker.db.url"),
    dbUser: String = conf.getString("jobbroker.db.user"),
    dbPassword: String = conf.getString("jobbroker.db.password"),
    mqHost: String = conf.getString("jobbroker.mq.host"),
    mqUser: String = conf.getString("jobbroker.mq.user"),
    mqPassword: String = conf.getString("jobbroker.mq.password")
  ): ClientContext = {
    val connectionFactory = new ConnectionFactory()
    connectionFactory.setUsername(mqUser)
    connectionFactory.setPassword(mqPassword)
    connectionFactory.setHost(mqHost)

    new ClientContext(
      () => DriverManager.getConnection(dbUrl, dbUser, dbPassword),
      () => connectionFactory.newConnection()
    )
  }
}
