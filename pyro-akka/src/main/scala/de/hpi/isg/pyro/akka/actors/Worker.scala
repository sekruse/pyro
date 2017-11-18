package de.hpi.isg.pyro.akka.actors

import java.util.concurrent.atomic.AtomicLong
import java.util.function

import akka.actor.{Actor, ActorLogging, Props}
import de.hpi.isg.pyro.akka.actors.NodeManager.WorkerStopped
import de.hpi.isg.pyro.akka.actors.Worker.DiscoveryTask
import de.hpi.isg.pyro.core.{ProfilingContext, SearchSpace}

import scala.language.implicitConversions

/**
  * This [[Actor]] performs actual profiling of a dataset.
  */
class Worker(profilingContext: ProfilingContext) extends Actor with ActorLogging {

  override def receive = {
    case DiscoveryTask(searchSpace) =>
      log.debug(s"Start processing $searchSpace...")
      val startMillis = System.currentTimeMillis
      searchSpace.discover()
      val elapsedMillis = System.currentTimeMillis - startMillis
      profilingContext.profilingData.operationMillis addAndGet elapsedMillis
      profilingContext.profilingData.searchSpaceMillis.computeIfAbsent(
        searchSpace,
        convertFunction((_: SearchSpace) => new AtomicLong(0L))
      ).addAndGet(elapsedMillis)
      log.debug(s"Stopped processing $searchSpace.")
      // Thread.sleep(scala.util.Random.nextInt(1000) + 1000)
      sender() ! WorkerStopped(searchSpace) // TODO: React appropriately.

    case msg => log.error(s"I am a dummy worker and cannot handle $msg.")
  }

  override def postStop() = {
    super.postStop()
    synchronized {
      profilingContext.profilingData.printReport(self.path.name, System.out)
    }
  }

  private implicit def convertFunction[A, B](f: A => B) =
    new function.Function[A, B] {
      override def apply(t: A): B = f(t)
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
