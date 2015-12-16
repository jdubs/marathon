package mesosphere.marathon.core.task.tracker.impl

import akka.actor.{ Actor, ActorRef, Props, Status }
import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.core.task.tracker.TaskTrackerConfig
import mesosphere.marathon.core.task.tracker.impl.TaskUpdateActor.{ FinishedTaskOp, ProcessTaskOp, TaskAction }
import mesosphere.marathon.state.{ PathId, TaskRepository }
import org.apache.mesos.Protos.TaskState._
import org.apache.mesos.Protos.TaskStatus
import org.slf4j.LoggerFactory

import scala.collection.immutable.Queue
import scala.concurrent.Future

object TaskUpdateActor {
  def props(config: TaskTrackerConfig, repo: TaskRepository): Props = {
    Props(new TaskUpdateActor(config, repo))
  }

  private[impl] case class ProcessTaskOp(sender: ActorRef, appId: PathId, taskId: String, action: TaskAction)
  private[impl] case class FinishedTaskOp(op: ProcessTaskOp)

  private[impl] sealed trait TaskAction

  private[impl] object TaskAction {
    case class Update(task: MarathonTask) extends TaskAction {
      override def toString: String = "Update/Create"
    }
    case class UpdateStatus(status: TaskStatus) extends TaskAction {
      override def toString: String = s"UpdateStatus ${status.getState}"
    }
    case object Expunge extends TaskAction
    case object Noop extends TaskAction
    case class Fail(cause: Throwable) extends TaskAction
  }
}

/**
  * Performs task updates to the repo and notifies the TaskTracker of the changes. After the TaskTracker
  * has processed the change, the change is confirmed to the sender.
  *
  * It serialized updates for the same task.
  *
  * Assumptions:
  * * This actor must be the only one performing task updates.
  * * This actor is spawned as a child of the [[TaskTrackerActor]].
  * * Errors in this actor lead to a restart of the TaskTrackerActor.
  */
private[impl] class TaskUpdateActor(config: TaskTrackerConfig, repo: TaskRepository) extends Actor {
  private[this] val log = LoggerFactory.getLogger(getClass)
  private[this] val taskTrackerDelegate = new TaskTrackerDelegate(config, context.parent)
  private[this] var queues = Map.empty[String, Queue[TaskUpdateActor.ProcessTaskOp]].withDefaultValue(Queue.empty)

  override def postStop(): Unit = {
    queues.values.iterator.flatten.map(_.sender) foreach { sender =>
      if (sender != context.system.deadLetters)
        sender ! Status.Failure(new IllegalStateException("TaskUpdateActor stopped"))
    }
  }

  def receive: Receive = {
    case op @ ProcessTaskOp(_, _, taskId, _) =>
      val oldQueue: Queue[ProcessTaskOp] = queues(taskId)
      val newQueue = oldQueue :+ op
      queues += taskId -> newQueue

      if (oldQueue.isEmpty)
        processNextOpIfExists(taskId)

    case FinishedTaskOp(op) =>
      val (dequeued, newQueue) = queues(op.taskId).dequeue
      require(dequeued == op)
      if (newQueue.isEmpty)
        queues -= op.taskId
      else
        queues += op.taskId -> newQueue

      log.debug(s"Finished processing ${op.action} for app [${op.appId}] and task [${op.taskId}]")

      processNextOpIfExists(op.taskId)

    case Status.Failure(cause) =>
      throw cause
  }

  private[this] def processNextOpIfExists(taskId: String): Unit = {
    import akka.pattern.pipe
    import context.dispatcher
    queues(taskId).headOption foreach { op =>
      log.debug(s"Start processing ${op.action} for app [${op.appId}] and task [${op.taskId}]")
      processOp(op).map { _ => FinishedTaskOp(op) }.pipeTo(self)
    }
  }

  private[this] def processOp(op: ProcessTaskOp): Future[_] = {
    val parent = context.parent
    import context.dispatcher

    op.action match {
      case TaskAction.Noop =>
        op.sender ! (())
        Future.successful(())
      case TaskAction.Fail(cause) =>
        op.sender ! Status.Failure(cause)
        Future.successful(())
      case TaskAction.Update(task) =>
        repo.store(task).map { _ =>
          parent ! TaskTrackerActor.TaskUpdated(op.appId, task, initiator = op.sender)
        }
      case TaskAction.Expunge =>
        repo.expunge(op.taskId).map { _ =>
          parent ! TaskTrackerActor.TaskRemoved(op.appId, op.taskId, initiator = op.sender)
        }
      case TaskAction.UpdateStatus(status) =>
        updateTaskStatusAction(op.appId, op.taskId, status).flatMap { action: TaskAction =>
          processOp(ProcessTaskOp(op.sender, op.appId, op.taskId, action))
        }
    }
  }

  private[this] def updateTaskStatusAction(appId: PathId, taskId: String, status: TaskStatus): Future[TaskAction] =
    {
      import context.dispatcher
      taskTrackerDelegate.appDataMapFuture.map { appData =>
        appData.appTasks(appId).taskMap.get(taskId) match {
          case Some(existingTask) =>
            actionForExistingTask(existingTask, status)
          case None =>
            TaskAction.Fail(new IllegalStateException(s"task [$taskId] of app [$appId] does not exist"))
        }
      }
    }

  private[this] def actionForExistingTask(task: MarathonTask, status: TaskStatus): TaskAction = {
    def updateTaskOnStateChange(task: MarathonTask): TaskAction = {
      def statusDidChange(statusA: TaskStatus, statusB: TaskStatus): Boolean = {
        val healthy = statusB.hasHealthy &&
          (!statusA.hasHealthy || statusA.getHealthy != statusB.getHealthy)

        healthy || statusA.getState != statusB.getState
      }

      if (statusDidChange(task.getStatus, status)) {
        TaskAction.Update(task.toBuilder.setStatus(status).build())
      }
      else {
        log.debug(s"Ignoring status update for ${task.getId}. Status did not change.")
        TaskAction.Noop
      }
    }

    status.getState match {
      case TASK_ERROR | TASK_FAILED | TASK_FINISHED | TASK_KILLED | TASK_LOST =>
        TaskAction.Expunge
      case TASK_RUNNING if !task.hasStartedAt => // was staged, is now running
        TaskAction.Update(task.toBuilder.setStartedAt(System.currentTimeMillis).setStatus(status).build())
      case _ =>
        updateTaskOnStateChange(task)
    }
  }
}
