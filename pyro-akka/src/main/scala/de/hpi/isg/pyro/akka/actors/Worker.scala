package de.hpi.isg.pyro.akka.actors

import akka.actor.{Actor, ActorLogging, Props}
import de.hpi.isg.pyro.akka.actors.NodeManager.WorkerStopped
import de.hpi.isg.pyro.akka.actors.Worker.DiscoveryTask
import de.hpi.isg.pyro.core.{ProfilingContext, SearchSpace}

/**
  * This [[Actor]] performs actual profiling of a dataset.
  */
class Worker(profilingContext: ProfilingContext) extends Actor with ActorLogging {

  override def receive = {
    case DiscoveryTask(searchSpace) =>
      log.info(s"Start processing $searchSpace...")
      val isSearchSpaceCleared = searchSpace.discover()
      log.info(s"Stopped processing $searchSpace.")
      // Thread.sleep(scala.util.Random.nextInt(1000) + 1000)
      sender() ! WorkerStopped(searchSpace, isSearchSpaceCleared) // TODO: React appropriately.

    case msg => log.error(s"I am a dummy worker and cannot handle $msg.")
  }

}

/**
  * Companion object.
  */
object Worker {

  /**
    * Create [[Props]] to create a new [[Worker]] actor.
    *
    * @param profilingContext with that the new actor should work
    * @return the [[Props]]
    */
  def props(profilingContext: ProfilingContext) = Props(classOf[Worker], profilingContext)

  /**
    * This message asks a [[Worker]] to profile a given [[SearchSpace]].
    *
    * @param searchSpace the [[SearchSpace]]
    */
  case class DiscoveryTask(searchSpace: SearchSpace)

}
