package mesosphere.marathon.core.leadership

import akka.actor.{ ActorRef, ActorRefFactory, Props }
import com.twitter.common.zookeeper.ZooKeeperClient
import mesosphere.marathon.LeadershipAbdication
import mesosphere.marathon.core.leadership.impl._

trait LeadershipModule {
  /**
    * Create a wrapper around an actor which should only be active if this instance of Marathon is the
    * current leader instance. The wrapper starts the actor with the given props when appropriate and stops
    * it when this instance loses Leadership. If the wrapper receives messages while not being the leader,
    * it answers all messages with Status.Failure messages.
    *
    * @param props the props to create the actor
    * @param name the name of the actor (the wrapping actor will be named like this)
    * @param considerPreparedOnStart whether the actor is ready to receive messages as soon as it is started.
    *                                if false, the actor expects a PreparationMessages.PrepareToStart message
    *                                and is assumed to be ready when it replies with PreparationMessages.Prepared.
    * @return
    */
  def startWhenLeader(props: Props, name: String, considerPreparedOnStart: Boolean = true): ActorRef = {

    def coordinator(): LeadershipCoordinator
}

/**
  * This module provides a utility function for starting actors only when our instance is the current leader.
  * This should be used for all normal top-level actors.
  *
  * In addition, it exports the coordinator which coordinates the activity performed when elected or stopped.
  * The leadership election logic needs to call the appropriate methods for this module to work.
  */
class LeadershipModuleImpl(actorRefFactory: ActorRefFactory, zk: ZooKeeperClient, leader: LeadershipAbdication) {

  private[this] var whenLeaderRefs = Set.empty[ActorRef]
  private[this] var started: Boolean = false

  /**
    * Create a wrapper around an actor which should only be active if this instance of Marathon is the
    * current leader instance. The wrapper starts the actor with the given props when appropriate and stops
    * it when this instance loses Leadership. If the wrapper receives messages while not being the leader,
    * it answers all messages with Status.Failure messages.
    *
    * @param props the props to create the actor
    * @param name the name of the actor (the wrapping actor will be named like this)
    * @param considerPreparedOnStart whether the actor is ready to receive messages as soon as it is started.
    *                                if false, the actor expects a PreparationMessages.PrepareToStart message
    *                                and is assumed to be ready when it replies with PreparationMessages.Prepared.
    * @return
    */
  def startWhenLeader(props: Props, name: String, considerPreparedOnStart: Boolean = true): ActorRef = {
    require(!started, "already started")
    val proxyProps = WhenLeaderActor.props(props, considerPreparedOnStart)
    val actorRef = actorRefFactory.actorOf(proxyProps, name)
    whenLeaderRefs += actorRef
    actorRef
  }

  def coordinator(): LeadershipCoordinator = coordinator_

  private[this] lazy val coordinator_ = {
    require(!started, "already started")
    started = true

    val props = LeadershipCoordinatorActor.props(whenLeaderRefs)
    val actorRef = actorRefFactory.actorOf(props, "leaderShipCoordinator")
    new LeadershipCoordinatorDelegate(actorRef)
  }

  /**
    * Register this actor by default.
    */
  startWhenLeader(AbdicateOnConnectionLossActor.props(zk, leader), "AbdicateOnConnectionLoss")
}
