package mesosphere.marathon.core.task.tracker.impl

import akka.actor.ActorRef
import akka.util.Timeout
import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.core.task.tracker.{ TaskCreator, TaskTrackerConfig, TaskUpdater }
import mesosphere.marathon.state.PathId
import org.apache.mesos.Protos.TaskStatus

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

private[tracker] class TaskCreatorAndUpdaterDelegate(conf: TaskTrackerConfig, taskTrackerRef: ActorRef)
    extends TaskCreator with TaskUpdater {

  import scala.concurrent.ExecutionContext.Implicits.global

  override def created(appId: PathId, task: MarathonTask): Future[MarathonTask] = {
    taskUpdate(appId, task.getId, TaskUpdateActor.TaskAction.Update(task)).map(_ => task)
  }
  override def terminated(appId: PathId, taskId: String): Future[_] = {
    taskUpdate(appId, taskId, TaskUpdateActor.TaskAction.Expunge)
  }
  override def statusUpdate(appId: PathId, status: TaskStatus): Future[_] = {
    taskUpdate(appId, status.getTaskId.getValue, TaskUpdateActor.TaskAction.UpdateStatus(status))
  }

  private[this] def taskUpdate(appId: PathId, taskId: String, action: TaskUpdateActor.TaskAction): Future[Unit] = {
    import akka.pattern.ask
    implicit val timeout: Timeout = conf.taskUpdateRequestTimeout().milliseconds
    (taskTrackerRef ? TaskTrackerActor.ForwardTaskOp(appId, taskId, action)).mapTo[Unit].recover {
      case NonFatal(e) =>
        throw new RuntimeException(s"while asking for $action on app [$appId] and task [$taskId]", e)
    }
  }
}
