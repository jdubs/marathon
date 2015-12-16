package mesosphere.marathon.core.task.tracker

import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.state.{ PathId, Timestamp }

import scala.collection.{ Iterable, Map }

/**
  * The TaskTracker exposes the latest known state for every task.
  *
  * It is an read-only interface. For modification, see
  * * [[TaskCreator]] for creating/removing tasks
  * * [[TaskUpdater]] for updating a task state according to a status update
  */
trait TaskTracker {

  def getTasks(appId: PathId): Iterable[MarathonTask]

  def getTask(appId: PathId, taskId: String): Option[MarathonTask]

  def getVersion(appId: PathId, taskId: String): Option[Timestamp] = {
    getTask(appId, taskId).map(task => Timestamp(task.getVersion))
  }

  def list: Map[PathId, TaskTracker.App]

  def count(appId: PathId): Int

  def contains(appId: PathId): Boolean
}

object TaskTracker {
  case class App(appName: PathId, tasks: Iterable[MarathonTask])
}
