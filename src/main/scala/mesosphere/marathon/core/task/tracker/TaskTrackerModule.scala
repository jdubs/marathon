package mesosphere.marathon.core.task.tracker

import mesosphere.marathon.core.leadership.LeadershipModule
import mesosphere.marathon.core.task.tracker.impl._
import mesosphere.marathon.state.TaskRepository

/**
  * Provides the interfaces to query the current task state ([[TaskTracker]]) and to
  * update the task state ([[TaskUpdater]], [[TaskCreator]]).
  */
class TaskTrackerModule(config: TaskTrackerConfig, leadershipModule: LeadershipModule, taskRepository: TaskRepository) {
  lazy val taskTracker: TaskTracker = new TaskTrackerDelegate(config, taskTrackerActorRef)

  def taskUpdater: TaskUpdater = taskTrackerCreatorAndUpdater
  def taskCreator: TaskCreator = taskTrackerCreatorAndUpdater

  private[this] lazy val taskUpdaterActorProps = TaskUpdateActor.props(config, taskRepository)
  private[this] lazy val taskLoader = new TaskLoaderImpl(taskRepository)
  private[this] lazy val taskTrackerActorProps = TaskTrackerActor.props(taskLoader, taskUpdaterActorProps)
  protected lazy val taskTrackerActorName = "taskTracker"
  private[this] lazy val taskTrackerActorRef = leadershipModule.startWhenLeader(
    taskTrackerActorProps, taskTrackerActorName
  )
  private[this] lazy val taskTrackerCreatorAndUpdater = new TaskCreatorAndUpdaterDelegate(config, taskTrackerActorRef)
}
