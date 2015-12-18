package mesosphere.marathon.test

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfter, Suite }

/**
  * Start an actor system before any test method.
  */
class MarathonActorSupport extends TestKit(ActorSystem("System")) with Suite with BeforeAndAfterAll {
  override protected def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}
