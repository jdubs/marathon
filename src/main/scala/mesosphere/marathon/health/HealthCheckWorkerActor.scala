package mesosphere.marathon.health

import mesosphere.marathon.state.{ AppDefinition, Timestamp }
import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.Protos.HealthCheckDefinition.Protocol.{
  COMMAND,
  HTTP,
  TCP,
  HTTPS
}
import mesosphere.marathon.tasks.MarathonTasks

import akka.actor.{ Actor, ActorLogging, PoisonPill }
import akka.util.Timeout

import spray.http._
import spray.client.pipelining._

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{ Success, Failure }

import java.net.{ Socket, InetSocketAddress }
import java.security.cert.X509Certificate
import javax.net.ssl.{ KeyManager, SSLContext, X509TrustManager }

class HealthCheckWorkerActor extends Actor with ActorLogging {

  import HealthCheckWorker._

  implicit val system = context.system
  implicit val ec = mesosphere.util.ThreadPoolContext.context // execution context for futures

  def receive: Receive = {
    case HealthCheckJob(app, task, check) =>
      val replyTo = sender() // avoids closing over the volatile sender ref

      doCheck(app, task, check)
        .andThen {
          case Success(Some(result)) => replyTo ! result
          case Success(None)         => // ignore
          case Failure(t) =>
            replyTo ! Unhealthy(
              task.getId,
              task.getVersion,
              s"${t.getClass.getSimpleName}: ${t.getMessage}"
            )
        }
        .onComplete { case _ => self ! PoisonPill }
  }

  def doCheck(app: AppDefinition, task: MarathonTask, check: HealthCheck): Future[Option[HealthResult]] =
    getPort(task, check) match {
      case None => Future.successful {
        Some(Unhealthy(task.getId, task.getVersion, "Missing/invalid port index and no explicit port specified"))
      }
      case Some(port) => check.protocol match {
        case HTTP  => http(app, task, check, port)
        case TCP   => tcp(app, task, check, port)
        case HTTPS => https(app, task, check, port)
        case COMMAND =>
          Future.failed {
            val message = s"COMMAND health checks can only be performed " +
              "by the Mesos executor."
            log.warning(message)
            new UnsupportedOperationException(message)
          }
        case _ =>
          Future.failed {
            val message = s"Unknown health check protocol: [${check.protocol}]"
            log.warning(message)
            new UnsupportedOperationException(message)
          }
      }
    }

  def getPort(task: MarathonTask, check: HealthCheck): Option[Int] = {
    def portViaIndex: Option[Int] = check.portIndex.flatMap(task.getPortsList.asScala.lift(_).map(_.toInt))
    check.port.orElse(portViaIndex)
  }

  def http(app: AppDefinition, task: MarathonTask, check: HealthCheck, port: Int): Future[Option[HealthResult]] = {
    val host = MarathonTasks.effectiveIpAddress(app, task)
    val rawPath = check.path.getOrElse("")
    val absolutePath = if (rawPath.startsWith("/")) rawPath else s"/$rawPath"
    val url = s"http://$host:$port$absolutePath"
    log.debug("Checking the health of [{}] via HTTP", url)

    def get(url: String): Future[HttpResponse] = {
      implicit val requestTimeout = Timeout(check.timeout)
      val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
      pipeline(Get(url))
    }

    get(url).map { response =>
      if (acceptableResponses contains response.status.intValue)
        Some(Healthy(task.getId, task.getVersion))
      else if (check.ignoreHttp1xx && (toIgnoreResponses contains response.status.intValue)) {
        log.debug(s"Ignoring health check HTTP response ${response.status.intValue} for task ${task.getId}")
        None
      }
      else {
        Some(Unhealthy(task.getId, task.getVersion, response.status.toString()))
      }
    }
  }

  def tcp(app: AppDefinition, task: MarathonTask, check: HealthCheck, port: Int): Future[Option[HealthResult]] = {
    val host = MarathonTasks.effectiveIpAddress(app, task)
    val address = s"$host:$port"
    val timeoutMillis = check.timeout.toMillis.toInt
    log.debug("Checking the health of [{}] via TCP", address)

    Future {
      val address = new InetSocketAddress(host, port)
      val socket = new Socket
      socket.connect(address, timeoutMillis)
      socket.close()
      Some(Healthy(task.getId, task.getVersion, Timestamp.now()))
    }
  }

  def https(app: AppDefinition, task: MarathonTask, check: HealthCheck, port: Int): Future[Option[HealthResult]] = {
    val host = MarathonTasks.effectiveIpAddress(app, task)
    val rawPath = check.path.getOrElse("")
    val absolutePath = if (rawPath.startsWith("/")) rawPath else s"/$rawPath"
    val url = s"https://$host:$port$absolutePath"
    log.debug("Checking the health of [{}] via HTTPS", url)

    def get(url: String): Future[HttpResponse] = {
      implicit val requestTimeout = Timeout(check.timeout)
      implicit def trustfulSslContext: SSLContext = {
        object BlindFaithX509TrustManager extends X509TrustManager {
          def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = ()
          def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = ()
          def getAcceptedIssuers: Array[X509Certificate] = Array[X509Certificate]()
        }

        val context = SSLContext.getInstance("Default")
        //scalastyle:off null
        context.init(Array[KeyManager](), Array(BlindFaithX509TrustManager), null)
        //scalastyle:on
        context
      }
      val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
      pipeline(Get(url))
    }

    get(url).map { response =>
      if (acceptableResponses contains response.status.intValue)
        Some(Healthy(task.getId, task.getVersion))
      else
        Some(Unhealthy(task.getId, task.getVersion, response.status.toString()))
    }
  }

}

object HealthCheckWorker {

  //scalastyle:off magic.number
  // Similar to AWS R53, we accept all responses in [200, 399]
  protected[health] val acceptableResponses = Range(200, 400)
  protected[health] val toIgnoreResponses = Range(100, 200)

  case class HealthCheckJob(app: AppDefinition, task: MarathonTask, check: HealthCheck)
}
