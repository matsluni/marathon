package mesosphere.marathon.core.launcher.impl

import com.codahale.metrics.MetricRegistry
import mesosphere.marathon.core.base.{ Clock, ConstantClock }
import mesosphere.marathon.core.launcher.{ OfferProcessor, OfferProcessorConfig, TaskLauncher }
import mesosphere.marathon.core.matcher.base.OfferMatcher
import OfferMatcher.{ MatchedTasks, TaskLaunchSource, TaskWithSource }
import mesosphere.marathon.core.matcher.base.OfferMatcher
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.state.{ PathId, Timestamp }
import mesosphere.marathon.tasks.{ TaskIdUtil, TaskTracker }
import mesosphere.marathon.{ MarathonSpec, MarathonTestHelper }
import mesosphere.util.Mockito
import mesosphere.util.state.PersistentEntity
import org.apache.mesos.Protos.TaskInfo
import org.mockito.Mockito.{ verify, when }
import org.scalatest.GivenWhenThen

import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

class OfferProcessorImplTest extends MarathonSpec with GivenWhenThen with Mockito {
  private[this] val offer = MarathonTestHelper.makeBasicOffer().build()
  private[this] val offerId = offer.getId
  private val appId: PathId = PathId("/testapp")
  private[this] val taskInfo1 = MarathonTestHelper.makeOneCPUTask(TaskIdUtil.taskId(appId)).build()
  private[this] val taskInfo2 = MarathonTestHelper.makeOneCPUTask(TaskIdUtil.taskId(appId)).build()
  private[this] val tasks = Seq(taskInfo1, taskInfo2)

  test("match successful, launch tasks successful") {
    Given("an offer")
    val dummySource = new DummySource
    val tasksWithSource = tasks.map(task => TaskWithSource(
      dummySource, task, MarathonTestHelper.makeTaskFromTaskInfo(task)))
    val offerProcessor = createProcessor()

    val deadline: Timestamp = clock.now() + 1.second

    And("an cooperative offerMatcher and taskTracker")
    offerMatcher.matchOffer(deadline, offer) returns Future.successful(MatchedTasks(offerId, tasksWithSource))
    for (task <- tasksWithSource) {
      taskTracker.store(appId, task.marathonTask) returns Future.successful(mock[PersistentEntity])
    }

    And("a working taskLauncher")
    taskLauncher.launchTasks(offerId, tasks) returns true

    When("processing the offer")
    Await.result(offerProcessor.processOffer(offer), 1.second)

    Then("we saw the offerMatch request and the task launches")
    verify(offerMatcher, times(1)).matchOffer(deadline, offer)
    verify(taskLauncher, times(1)).launchTasks(offerId, tasks)

    And("all task launches have been accepted")
    assert(dummySource.rejected.isEmpty)
    assert(dummySource.accepted.toSeq == tasks)

    And("the tasks have been stored")
    for (task <- tasksWithSource) {
      verify(taskTracker, times(1)).created(appId, task.marathonTask)
      verify(taskTracker, times(1)).store(appId, task.marathonTask)
    }
  }

  test("match successful, launch tasks unsuccessful") {
    Given("an offer")
    val dummySource = new DummySource
    val tasksWithSource = tasks.map(task => TaskWithSource(
      dummySource, task, MarathonTestHelper.makeTaskFromTaskInfo(task)))

    val offerProcessor = createProcessor()

    val deadline: Timestamp = clock.now() + 1.second
    And("an cooperative offerMatcher and taskTracker")
    offerMatcher.matchOffer(deadline, offer) returns Future.successful(MatchedTasks(offerId, tasksWithSource))
    for (task <- tasksWithSource) {
      taskTracker.store(appId, task.marathonTask) returns Future.successful(mock[PersistentEntity])
      taskTracker.terminated(appId, task.marathonTask.getId) returns Future.successful(Some(task.marathonTask))
    }

    And("a dysfunctional taskLauncher")
    taskLauncher.launchTasks(offerId, tasks) returns false

    When("processing the offer")
    Await.result(offerProcessor.processOffer(offer), 1.second)

    Then("we saw the matchOffer request and the task launch attempt")
    verify(offerMatcher, times(1)).matchOffer(deadline, offer)
    verify(taskLauncher, times(1)).launchTasks(offerId, tasks)

    And("all task launches were rejected")
    assert(dummySource.accepted.isEmpty)
    assert(dummySource.rejected.toSeq.map(_._1) == tasks)

    And("the tasks where first stored and then expunged again")
    for (task <- tasksWithSource) {
      verify(taskTracker, times(1)).created(appId, task.marathonTask)
      verify(taskTracker, times(1)).store(appId, task.marathonTask)
      verify(taskTracker, times(1)).terminated(appId, task.marathonTask.getId)
    }
  }

  test("match successful but very slow so that we are hitting storage timeout") {
    Given("an offer")
    val dummySource = new DummySource
    val tasksWithSource = tasks.map(task => TaskWithSource(
      dummySource, task, MarathonTestHelper.makeTaskFromTaskInfo(task)))

    val offerProcessor = createProcessor()

    val deadline: Timestamp = clock.now() + 1.second
    And("an cooperative offerMatcher that takes really long")
    offerMatcher.matchOffer(deadline, offer) answers { _ =>
      // advance clock "after" match
      clock += 1.hour
      Future.successful(MatchedTasks(offerId, tasksWithSource))
    }

    When("processing the offer")
    Await.result(offerProcessor.processOffer(offer), 1.second)

    Then("we saw the matchOffer request")
    verify(offerMatcher, times(1)).matchOffer(deadline, offer)

    And("all task launches were rejected")
    assert(dummySource.accepted.isEmpty)
    assert(dummySource.rejected.toSeq.map(_._1) == tasks)

    And("no tasks where stored")
    noMoreInteractions(taskTracker)
  }

  test("match successful but first store is so slow that we are hitting storage timeout") {
    Given("an offer")
    val dummySource = new DummySource
    val tasksWithSource = tasks.map(task => TaskWithSource(
      dummySource, task, MarathonTestHelper.makeTaskFromTaskInfo(task)))

    val offerProcessor = createProcessor()

    val deadline: Timestamp = clock.now() + 1.second
    And("a cooperative taskLauncher")
    taskLauncher.launchTasks(offerId, tasks.take(1)) returns true

    And("an cooperative offerMatcher")
    offerMatcher.matchOffer(deadline, offer) returns Future.successful(MatchedTasks(offerId, tasksWithSource))

    for (task <- tasksWithSource) {
      taskTracker.store(appId, task.marathonTask) answers { args =>
        // simulate that stores are really slow
        clock += 1.hour
        Future.successful(mock[PersistentEntity])
      }
      taskTracker.terminated(appId, task.marathonTask.getId) returns Future.successful(Some(task.marathonTask))
    }

    When("processing the offer")
    Await.result(offerProcessor.processOffer(offer), 1.second)

    Then("we saw the matchOffer request and the task launch attempt for the first task")
    verify(offerMatcher, times(1)).matchOffer(deadline, offer)
    verify(taskLauncher, times(1)).launchTasks(offerId, tasks.take(1))

    And("one task launch was accepted")
    assert(dummySource.accepted.toSeq == tasks.take(1))

    And("one task launch was rejected")
    assert(dummySource.rejected.toSeq.map(_._1) == tasks.drop(1))

    And("the first task was stored")
    for (task <- tasksWithSource.take(1)) {
      verify(taskTracker, times(1)).created(appId, task.marathonTask)
      verify(taskTracker, times(1)).store(appId, task.marathonTask)
    }

    And("and the second task was not stored")
    noMoreInteractions(taskTracker)
  }

  test("match empty => decline") {
    val offerProcessor = createProcessor()

    val deadline: Timestamp = clock.now() + 1.second
    offerMatcher.matchOffer(deadline, offer) returns Future.successful(MatchedTasks(offerId, Seq.empty))

    Await.result(offerProcessor.processOffer(offer), 1.second)

    verify(offerMatcher, times(1)).matchOffer(deadline, offer)
    verify(taskLauncher, times(1)).declineOffer(offerId, refuseMilliseconds = Some(conf.declineOfferDuration()))
  }

  test("match crashed => decline") {
    val offerProcessor = createProcessor()

    val deadline: Timestamp = clock.now() + 1.second
    offerMatcher.matchOffer(deadline, offer) returns Future.failed(new RuntimeException("failed matching"))

    Await.result(offerProcessor.processOffer(offer), 1.second)

    verify(offerMatcher, times(1)).matchOffer(deadline, offer)
    verify(taskLauncher, times(1)).declineOffer(offerId, refuseMilliseconds = None)
  }

  private[this] var clock: ConstantClock = _
  private[this] var offerMatcher: OfferMatcher = _
  private[this] var taskLauncher: TaskLauncher = _
  private[this] var taskTracker: TaskTracker = _
  private[this] var conf: OfferProcessorConfig = _

  private[this] def createProcessor(): OfferProcessor = {
    conf = new OfferProcessorConfig {}
    conf.afterInit()

    clock = ConstantClock()
    offerMatcher = mock[OfferMatcher]
    taskLauncher = mock[TaskLauncher]
    taskTracker = mock[TaskTracker]

    new OfferProcessorImpl(conf, clock, new Metrics(new MetricRegistry), offerMatcher, taskLauncher, taskTracker)
  }

  class DummySource extends TaskLaunchSource {
    var rejected = Vector.empty[(TaskInfo, String)]
    var accepted = Vector.empty[TaskInfo]

    override def taskLaunchRejected(taskInfo: TaskInfo, reason: String): Unit = rejected :+= taskInfo -> reason
    override def taskLaunchAccepted(taskInfo: TaskInfo): Unit = accepted :+= taskInfo
  }
}
