package mesosphere.marathon.core.task.tracker.impl

import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.state.TaskRepository
import mesosphere.marathon.tasks.TaskIdUtil
import org.slf4j.LoggerFactory

import scala.concurrent.Future

private[tracker] class TaskLoaderImpl(repo: TaskRepository) extends TaskLoader {
  import scala.concurrent.ExecutionContext.Implicits.global

  private[this] val log = LoggerFactory.getLogger(getClass.getName)

  override def loadTasks(): Future[AppDataMap] = {
    def fetchTask(taskId: String): Future[Option[MarathonTask]] = repo.task(taskId)

    val namesFuture = repo.allIds()
    for {
      names <- namesFuture
      _ = log.info(s"About to load ${names.size} tasks")
      tasks <- Future.sequence(names.map(fetchTask)).map(_.flatten)
    } yield {
      log.info(s"Loaded ${tasks.size} tasks")
      val tasksByApp = tasks.groupBy(task => TaskIdUtil.appId(task.getId))
      val map = tasksByApp.iterator.map {
        case (appId, appTasks) =>
          appId -> AppData(appId, appTasks.map(task => task.getId -> task).toMap)
      }.toMap
      AppDataMap.of(map)
    }
  }
}
