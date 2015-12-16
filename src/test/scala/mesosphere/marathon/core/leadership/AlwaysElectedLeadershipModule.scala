package mesosphere.marathon.core.leadership

import akka.actor.{ ActorSystem, ActorRefFactory, ActorRef, Props }
import com.twitter.common.zookeeper.ZooKeeperClient
import mesosphere.marathon.LeadershipAbdication
import mesosphere.marathon.core.base.{ ActorsModule, ShutdownHooks }
import mesosphere.marathon.test.Mockito
import org.apache.zookeeper.ZooKeeper

/**
  * Provides an implementation of the [[LeadershipModule]] which assumes that we are always the leader.
  *
  * This simplifies tests.
  */
object AlwaysElectedLeadershipModule extends Mockito {
  def apply(shutdownHooks: ShutdownHooks = ShutdownHooks()): LeadershipModule = {
    forActorsModule(new ActorsModule(shutdownHooks))
  }

  def forActorSystem(actorSystem: ActorSystem): LeadershipModule = {
    forActorsModule(new ActorsModule(ShutdownHooks(), actorSystem))
  }

  def forActorsModule(actorsModule: ActorsModule = new ActorsModule(ShutdownHooks())): LeadershipModule = {
    val zkClient = mock[ZooKeeperClient]
    val zooKeeper: ZooKeeper = mock[ZooKeeper]
    zooKeeper.getState returns ZooKeeper.States.CONNECTED
    zkClient.get(any) returns zooKeeper
    val leader = mock[LeadershipAbdication]
    new AlwaysElectedLeadershipModule(actorsModule.actorRefFactory, zkClient, leader)
  }
}

private class AlwaysElectedLeadershipModule(
  actorRefFactory: ActorRefFactory, zk: ZooKeeperClient, leader: LeadershipAbdication)
    extends LeadershipModule(actorRefFactory, zk, leader) {
  override def startWhenLeader(props: Props, name: String, preparedOnStart: Boolean = true): ActorRef =
    actorRefFactory.actorOf(props, name)
  override def coordinator(): LeadershipCoordinator = ???
}
