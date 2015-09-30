package mesosphere.marathon.tasks

import com.fasterxml.uuid.{ EthernetAddress, Generators }
import mesosphere.marathon.state.PathId
import org.apache.mesos.Protos.TaskID

/**
  * Utility functions for dealing with TaskIDs
  */
class TaskIdUtil {
  val appDelimiter = "."
  val TaskIdRegex = """^(.+)[\._]([^_\.]+)$""".r
  val uuidGenerator = Generators.timeBasedGenerator(EthernetAddress.fromInterface())

  def appId(taskId: TaskID): PathId = appId(taskId.getValue)

  // this should actually be a try or option
  def appId(taskId: String): PathId = {
    taskId match {
      case TaskIdRegex(appId, uuid) => PathId.fromSafePath(appId)
      case _                        => throw new MatchError(s"taskId $taskId is no valid identifier")
    }
  }

  def newTaskId(appId: PathId): TaskID = {
    val taskId = appId.safePath + appDelimiter + uuidGenerator.generate()
    TaskID.newBuilder()
      .setValue(taskId)
      .build
  }

  def taskBelongsTo(appId: PathId)(taskId: String): Boolean = taskId.startsWith(appId.safePath + appDelimiter)

}

object TaskIdUtil extends TaskIdUtil
