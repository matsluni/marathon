package mesosphere.marathon.api.v2

import java.util.Collections

import mesosphere.marathon.api.{ TaskKiller, TestAuthFixture }
import mesosphere.marathon.health.HealthCheckManager
import mesosphere.marathon.state.PathId.StringPathId
import mesosphere.marathon.state.{ GroupManager, Timestamp }
import mesosphere.marathon.tasks.{ MarathonTasks, TaskIdUtil, TaskTracker }
import mesosphere.marathon.{ BadRequestException, MarathonConf, MarathonSchedulerService, MarathonSpec }
import mesosphere.mesos.protos.Implicits._
import mesosphere.mesos.protos._
import mesosphere.util.Mockito
import org.mockito.Mockito._
import org.scalatest.{ GivenWhenThen, Matchers }

import scala.concurrent.duration._

class TasksResourceTest extends MarathonSpec with GivenWhenThen with Matchers with Mockito {

  test("killTasks") {
    Given("two apps and 1 task each")
    val app1 = "/my/app-1".toRootPath
    val app2 = "/my/app-2".toRootPath
    val taskId1 = taskIdUtil.newTaskId(app1).getValue
    val taskId2 = taskIdUtil.newTaskId(app2).getValue
    val body = s"""{"ids": ["$taskId1", "$taskId2"]}"""
    val bodyBytes = body.toCharArray.map(_.toByte)

    val slaveId = SlaveID("some slave ID")
    val now = Timestamp.now()
    val task1 = MarathonTasks.makeTask(
      taskId1, "host", ports = Nil, attributes = Nil, version = Timestamp.now(), now = now,
      slaveId = slaveId
    )
    val task2 = MarathonTasks.makeTask(
      taskId2, "host", ports = Nil, attributes = Nil, version = Timestamp.now(), now = now,
      slaveId = slaveId
    )

    config.zkTimeoutDuration returns 5.seconds
    taskTracker.fetchTask(taskId1) returns Some(task1)
    taskTracker.fetchTask(taskId2) returns Some(task2)

    When("we ask to kill both tasks")
    val response = taskResource.killTasks(scale = false, body = bodyBytes, auth.request, auth.response)

    Then("The response should be OK")
    response.getStatus shouldEqual 200

    And("Both tasks should be requested to be killed")
    verify(taskKiller).kill(eq(app1), any, force = eq(true))
    verify(taskKiller).kill(eq(app2), any, force = eq(true))

    And("nothing else should be called on the TaskKiller")
    noMoreInteractions(taskKiller)
  }

  test("access without authentication is denied") {
    Given("An unauthenticated request")
    auth.authenticated = false
    val req = auth.request
    val resp = auth.response
    val taskId1 = taskIdUtil.newTaskId("/my/app".toRootPath).getValue
    val taskId2 = taskIdUtil.newTaskId("/my/app".toRootPath).getValue
    val taskId3 = taskIdUtil.newTaskId("/my/app".toRootPath).getValue
    val body = s"""{"ids": ["$taskId1", "$taskId2", "$taskId3"]}""".getBytes

    When(s"the index as json is fetched")
    val running = taskResource.indexJson("status", Collections.emptyList(), req, resp)
    Then("we receive a NotAuthenticated response")
    running.getStatus should be(auth.NotAuthenticatedStatus)

    When(s"one index as txt is fetched")
    val cancel = taskResource.indexTxt(req, resp)
    Then("we receive a NotAuthenticated response")
    cancel.getStatus should be(auth.NotAuthenticatedStatus)

    When(s"kill task is called")
    val killTasks = taskResource.killTasks(true, body, req, resp)
    Then("we receive a NotAuthenticated response")
    killTasks.getStatus should be(auth.NotAuthenticatedStatus)
  }

  test("access without authorization is denied") {
    Given("An unauthorized request")
    auth.authenticated = true
    auth.authorized = false
    val req = auth.request
    val resp = auth.response
    val taskId1 = taskIdUtil.newTaskId("/my/app".toRootPath).getValue
    val taskId2 = taskIdUtil.newTaskId("/my/app".toRootPath).getValue
    val taskId3 = taskIdUtil.newTaskId("/my/app".toRootPath).getValue
    val body = s"""{"ids": ["$taskId1", "$taskId2", "$taskId3"]}""".getBytes

    When(s"kill task is called")
    val killTasks = taskResource.killTasks(true, body, req, resp)
    Then("we receive a not authorized response")
    killTasks.getStatus should be(auth.UnauthorizedStatus)
  }

  test("killTasks fails for invalid taskId") {
    Given("a valid and an invalid taskId")
    val app1 = "/my/app-1".toRootPath
    val taskId1 = taskIdUtil.newTaskId(app1).getValue
    val body = s"""{"ids": ["$taskId1", "invalidTaskId"]}"""
    val bodyBytes = body.toCharArray.map(_.toByte)

    When("we ask to kill those two tasks")
    val ex = intercept[BadRequestException] {
      taskResource.killTasks(scale = false, body = bodyBytes, auth.request, auth.response)
    }

    Then("An exception should be thrown that points to the invalid taskId")
    ex.getMessage should include ("invalidTaskId")

    And("the taskKiller should not be called at all")
    verifyNoMoreInteractions(taskKiller)
  }

  var service: MarathonSchedulerService = _
  var taskTracker: TaskTracker = _
  var taskKiller: TaskKiller = _
  var config: MarathonConf = _
  var groupManager: GroupManager = _
  var healthCheckManager: HealthCheckManager = _
  var taskResource: TasksResource = _
  var taskIdUtil: TaskIdUtil = TaskIdUtil
  var auth: TestAuthFixture = _

  before {
    auth = new TestAuthFixture
    service = mock[MarathonSchedulerService]
    taskTracker = mock[TaskTracker]
    taskKiller = mock[TaskKiller]
    config = mock[MarathonConf]
    groupManager = mock[GroupManager]
    healthCheckManager = mock[HealthCheckManager]
    taskResource = new TasksResource(
      service,
      taskTracker,
      taskKiller,
      config,
      groupManager,
      healthCheckManager,
      taskIdUtil,
      auth.auth,
      auth.auth
    )
  }

}
