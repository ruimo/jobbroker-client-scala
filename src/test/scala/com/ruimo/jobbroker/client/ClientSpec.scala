package com.ruimo.jobbroker.client

import java.io.ByteArrayInputStream

import org.specs2.mutable._
import java.sql.DriverManager
import java.time.Instant

import com.ruimo.scoins.Scoping._
import com.github.fridujo.rabbitmq.mock.MockConnectionFactory
import com.ruimo.jobbroker.JobId
import com.ruimo.jobbroker.dao.{AccountId, ApplicationId, Migration, Request}
import com.ruimo.jobbroker.queue.WaitingJobHandle
import org.specs2.execute.{ExecuteException, FailureException}

import scala.util.Random

class ClientSpec extends Specification {
  "Client" should {
    "Can submit and retrieve job by an array of byte." in {
      org.h2.Driver.load()
      val conn = DriverManager.getConnection("jdbc:h2:mem:test" + Random.nextLong())
      Migration.perform(conn)
      val mqFactory = new MockConnectionFactory

      var resp: Option[(Request, Array[Byte])] = None

      def f(client: Client) {
        val req: Request = client.submitJobWithBytes(
          AccountId("acc01"),
          ApplicationId("app01"),
          "Hello".getBytes("utf-8"),
          Instant.ofEpochMilli(123L)
        )

        val handle: WaitingJobHandle = client.retrieveJobWithBytes(
          (request: Request, bytes: Array[Byte]) => {resp = Some(request, bytes)},
          () => {failure("onCancel should not be called.")},
          (jobId: JobId, t: Throwable) => {
            t.printStackTrace()
            failure("onError should not be called.")
          },
          Instant.ofEpochMilli(234L)
        )

        val start = System.currentTimeMillis
        while (! resp.isDefined) {
          Thread.sleep(100)
          if (System.currentTimeMillis - start > 10000) failure("Time out")
        }
      }

      (new ClientContext(() => conn, () => mqFactory.newConnection)).withClient(f)
      resp.isDefined === true
      doWith(resp.get) { case (req, bytes) =>
          new String(bytes, "utf-8") === "Hello"
          req.accountId === AccountId("acc01")
          req.applicationId === ApplicationId("app01")
          req.acceptedTime === Instant.ofEpochMilli(123L)
          req.jobStartTime === Some(Instant.ofEpochMilli(234L))
          req.jobEndTime === None
      }
    }

    "Can submit and retrieve job by a stream of byte." in {
      org.h2.Driver.load()
      val conn = DriverManager.getConnection("jdbc:h2:mem:test" + Random.nextLong())
      Migration.perform(conn)
      val mqFactory = new MockConnectionFactory

      var resp: Option[(Request, Array[Byte])] = None

      def f(client: Client) {
        val req: Request = client.submitJobWithStream(
          AccountId("acc01"),
          ApplicationId("app01"),
          new ByteArrayInputStream("Hello".getBytes("utf-8")),
          Instant.ofEpochMilli(123L)
        )

        val handle: WaitingJobHandle = client.retrieveJobWithBytes(
          (request: Request, bytes: Array[Byte]) => {resp = Some(request, bytes)},
          () => {failure("onCancel should not be called.")},
          (jobId: JobId, t: Throwable) => {
            t.printStackTrace()
            failure("onError should not be called.")
          },
          Instant.ofEpochMilli(234L)
        )

        val start = System.currentTimeMillis
        while (! resp.isDefined) {
          Thread.sleep(100)
          if (System.currentTimeMillis - start > 10000) failure("Time out")
        }
      }

      (new ClientContext(() => conn, () => mqFactory.newConnection)).withClient(f)

      resp.isDefined === true
      doWith(resp.get) { case (req, bytes) =>
          new String(bytes, "utf-8") === "Hello"
          req.accountId === AccountId("acc01")
          req.applicationId === ApplicationId("app01")
          req.acceptedTime === Instant.ofEpochMilli(123L)
          req.jobStartTime === Some(Instant.ofEpochMilli(234L))
          req.jobEndTime === None
      }
    }

    "Be able to cancel job by an array of byte." in {
      org.h2.Driver.load()
      val conn = DriverManager.getConnection("jdbc:h2:mem:test" + Random.nextLong())
      Migration.perform(conn)
      val mqFactory = new MockConnectionFactory

      var onCancelCalled: Boolean = false

      def f(client: Client) {
        val handle: WaitingJobHandle = client.retrieveJobWithBytes(
          (request: Request, bytes: Array[Byte]) => {failure("onJobObtained should not be called")},
          () => {onCancelCalled = true},
          (jobId: JobId, t: Throwable) => {
            t.printStackTrace()
            failure("onError should not be called.")
          },
          Instant.ofEpochMilli(234L)
        )

        client.cancelJobWaiting(handle)
      }

      (new ClientContext(() => conn, () => mqFactory.newConnection)).withClient(f)

      val start = System.currentTimeMillis
      while (! onCancelCalled) {
        Thread.sleep(100)
        if (System.currentTimeMillis - start > 10000) failure("Time out")
      }

      1 === 1
    }

    "Error should be treated." in {
      org.h2.Driver.load()
      val conn = DriverManager.getConnection("jdbc:h2:mem:test" + Random.nextLong())
      Migration.perform(conn)
      val mqFactory = new MockConnectionFactory

      var onErrorCalled: Option[(JobId, Throwable)] = None
      val error = new Throwable

      def f0(client: Client) {
        val req: Request = client.submitJobWithStream(
          AccountId("acc01"),
          ApplicationId("app01"),
          new ByteArrayInputStream("Hello".getBytes("utf-8")),
          Instant.ofEpochMilli(123L)
        )

        val handle: WaitingJobHandle = client.retrieveJobWithBytes(
          (request: Request, bytes: Array[Byte]) => {
            throw error
          },
          () => {failure("onCancel should not be called.")},
          (jobId: JobId, t: Throwable) => {
            onErrorCalled = Some(jobId, t)
          },
          Instant.ofEpochMilli(234L)
        )

        val start = System.currentTimeMillis
        while (! onErrorCalled.isDefined) {
          Thread.sleep(100)
          if (System.currentTimeMillis - start > 10000) failure("Time out")
        }
      }

      (new ClientContext(() => conn, () => mqFactory.newConnection)).withClient(f0)

      1 === 1
    }

    "Can submit and retrieve and store result by an array of byte." in {
      org.h2.Driver.load()
      val conn = DriverManager.getConnection("jdbc:h2:mem:test" + Random.nextLong())
      Migration.perform(conn)
      val mqFactory = new MockConnectionFactory

      @volatile var result: Option[(Request, Array[Byte])] = None

      def f(client: Client) {
        val req: Request = client.submitJobWithBytes(
          AccountId("acc01"),
          ApplicationId("app01"),
          "Ruimo".getBytes("utf-8"),
          Instant.ofEpochMilli(123L)
        )

        val handle: WaitingJobHandle = client.retrieveJobWithBytes(
          (request: Request, bytes: Array[Byte]) => {
            client.storeJobResultWithBytes(
              request.id, ("Hello," + new String(bytes, "utf-8")).getBytes("utf-8"), now = Instant.ofEpochMilli(999L)
            )
            result = Some(Request.retrieveJobResultWithBytes(request.id)(conn))
          },
          () => {failure("onCancel should not be called.")},
          (jobId: JobId, t: Throwable) => {
            t.printStackTrace()
            failure("onError should not be called.")
          },
          Instant.ofEpochMilli(234L)
        )

        val start = System.currentTimeMillis
        while (! result.isDefined) {
          Thread.sleep(100)
          if (System.currentTimeMillis - start > 10000) failure("Time out")
        }
      }

      (new ClientContext(() => conn, () => mqFactory.newConnection)).withClient(f)

      doWith(result.get) { case (req, bytes) =>
          new String(bytes, "utf-8") === "Hello,Ruimo"
          req.accountId === AccountId("acc01")
          req.applicationId === ApplicationId("app01")
          req.acceptedTime === Instant.ofEpochMilli(123L)
          req.jobStartTime === Some(Instant.ofEpochMilli(234L))
          req.jobEndTime === Some(Instant.ofEpochMilli(999L))
      }
    }
  }
}
