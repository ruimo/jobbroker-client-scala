package com.ruimo.jobbroker.client

import java.io.{InputStream, PrintWriter, StringWriter}
import java.sql.{Connection, DriverManager}
import java.time.Instant

import com.rabbitmq.client.{Channel, ConnectionFactory}
import com.rabbitmq.client.{Connection => MqConnection}
import com.ruimo.jobbroker.JobId
import com.ruimo.jobbroker.dao._
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

  def doDbMigration() {
    Migration.perform(conn())
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
