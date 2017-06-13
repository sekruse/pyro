package de.hpi.isg.pyro.akka.actors

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import de.hpi.isg.pyro.akka.messages.{NodeManagerReady, StartProfiling, WorkersReady}

import scala.concurrent.duration.FiniteDuration

/**
  * There should be one such [[Actor]] on each machine. It's purpose is to control the resources and profiling process
  * on that very node.
  */
class NodeManager extends Actor with Printing {


  /**
    * Reference to the [[Controller]] of the profiling process.
    */
  var controller: ActorRef = _

  /**
    * References to the [[Worker]]s under this instance.
    */
  var workers: Array[ActorRef] = _

  override def preStart(): Unit = {
    super.preStart()
    controller = context.parent
  }

  override def receive = {
    case StartProfiling(path, hosts) => startProfiling(path)
    case other => sys.error(s"[${self.path}] Unknown message: $other")
  }

  /**
    * Initiates the profiling process.
    *
    * @param path of the dataset to be profiled
    */
  private def startProfiling(path: String) = {
      actorPrint(s"Profiling $path for ${controller.path}.")

      // Allocate the workers.
      val parallelism = 4 // TODO
      workers = (0 until parallelism).map(i => context.actorOf(Props[Worker], f"worker-$i%04d")).toArray
      actorPrint(s"Started $parallelism workers.")

      controller ! WorkersReady(workers.length)
  }

}

object NodeManager {

  def createOn(actorSystem: ActorSystem) = actorSystem.actorOf(Props[NodeManager], "node-manager")

}
