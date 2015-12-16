package mesosphere.marathon.core.task.tracker.impl
import scala.concurrent.Future

private[tracker] trait TaskLoader {
  def loadTasks(): Future[AppDataMap]
}
