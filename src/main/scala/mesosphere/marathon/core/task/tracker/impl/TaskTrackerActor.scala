package mesosphere.marathon.core.task.tracker.impl

import akka.actor.SupervisorStrategy.Escalate
import akka.actor._
import akka.event.LoggingReceive
import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.core.task.tracker.impl.TaskTrackerActor.ForwardTaskOp
import mesosphere.marathon.state.PathId
import org.slf4j.LoggerFactory

object TaskTrackerActor {
  def props(taskLoader: TaskLoader, taskUpdaterProps: Props): Props = {
    Props(new TaskTrackerActor(taskLoader, taskUpdaterProps))
  }

  private[impl] case object List
  private[impl] case class ForwardTaskOp(appId: PathId, taskId: String, action: TaskUpdateActor.TaskAction)

  private[impl] case class TaskUpdated(appId: PathId, task: MarathonTask, initiator: ActorRef)
  private[impl] case class TaskRemoved(appId: PathId, taskId: String, initiator: ActorRef)
}

/**
  * Holds the current in-memory version of all task state. It gets informed of task state changes
  * after they have been persisted.
  *
  * Updating is serialized via the [[TaskUpdateActor]] which also initiates the required persistence operations.
  */
private class TaskTrackerActor(taskLoader: TaskLoader, taskUpdaterProps: Props) extends Actor with Stash {
  private[this] val log = LoggerFactory.getLogger(getClass)
  private[this] val updaterRef = context.actorOf(taskUpdaterProps, "updater")

  override val supervisorStrategy = OneForOneStrategy() { case _: Exception => Escalate }

  override def preStart(): Unit = {
    log.info(s"${getClass.getSimpleName} is starting. Task loading initiated.")

    import akka.pattern.pipe
    import context.dispatcher
    taskLoader.loadTasks().pipeTo(self)
  }

  override def receive: Receive = initializing

  private[this] def initializing: Receive = LoggingReceive.withLabel("initializing") {
    case appTasks: AppDataMap =>
      log.info("Task loading complete.")

      unstashAll()
      context.become(withTasks(appTasks))

    case Status.Failure(cause) =>
      // escalate this failure
      throw new IllegalStateException("while loading tasks", cause)

    case stashMe: AnyRef =>
      stash()
  }

  private[this] def withTasks(appTasks: AppDataMap): Receive = {
    def becomeWithUpdatedApp(appId: PathId)(update: AppData => AppData): Unit = {
      context.become(withTasks(appTasks.updateApp(appId)(update)))
    }

    LoggingReceive.withLabel("withTasks") {
      case TaskTrackerActor.List =>
        sender() ! appTasks

      case ForwardTaskOp(appId, taskId, action) =>
        updaterRef.forward(TaskUpdateActor.ProcessTaskOp(sender(), appId, taskId, action))

      case msg @ TaskTrackerActor.TaskUpdated(appId, task, initiator) =>
        becomeWithUpdatedApp(appId)(_.withTask(task))
        initiator ! (())

      case msg @ TaskTrackerActor.TaskRemoved(appId, taskId, initiator) =>
        becomeWithUpdatedApp(appId)(_.withoutTask(taskId))
        initiator ! (())
    }
  }
}
