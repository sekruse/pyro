package de.hpi.isg.pyro.akka.actors

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.routing.SmallestMailboxPool
import de.hpi.isg.pyro.akka.messages.{ColumnReport, NodeManagerState, ProfilingTask}
import de.hpi.isg.pyro.core.{Configuration, ProfilingContext, SearchSpace}
import de.hpi.isg.pyro.model.{ColumnLayoutRelation, PartialFD, PartialKey}
import de.metanome.algorithm_integration.input.RelationalInputGenerator
import de.hpi.isg.pyro.akka.utils.JavaScalaCompatibility._

import scala.collection.mutable
import scala.collection.JavaConversions._

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
    * Reference to the [[Worker]] pool under this instance.
    */
  var workerPool: ActorRef = _

  /**
    * Number of idle [[Worker]]s.
    */
  var numIdleWorkers: Int = _

  /**
    * Maintains data to do the profiling upon.
    */
  var profilingContext: ProfilingContext = _

  /**
    * Maintains search spaces to be processed by this instance.
    */
  var searchSpaces: mutable.Set[SearchSpace] = mutable.Set()

  override def preStart(): Unit = {
    super.preStart()
    controller = context.parent
  }

  override def receive = {
    case ProfilingTask(configuration, inputPath, inputGenerator) =>
      startProfiling(configuration, inputPath, inputGenerator)

    case searchSpaces: Seq[SearchSpace] =>
      val newSearchSpaces = searchSpaces filterNot this.searchSpaces
      this.searchSpaces ++= newSearchSpaces

    case other => sys.error(s"[${self.path}] Unknown message: $other")
  }

  /**
    * Initiates the profiling process.
    *
    * @param configuration defines what to profile and how
    */
  private def startProfiling(configuration: Configuration,
                             inputPath: String,
                             inputGenerator: Option[RelationalInputGenerator]) = {
    actorPrint(s"Profiling $inputPath for ${controller.path}.")

    // Obtain the relation.
    val relation = inputGenerator match {
      case Some(generator) =>
        actorPrint(s"Loading relation from $generator...")
        ColumnLayoutRelation.createFrom(generator,
          configuration.isNullEqualNull,
          configuration.maxCols,
          configuration.maxRows
        )

      case None =>
        sys.error(s"No RelationalInputGenerator not supported.")
    }
    controller ! ColumnReport(relation.getColumns)

    profilingContext = new ProfilingContext(
      configuration,
      relation,
      (ucc: PartialKey) => actorPrint(s"Discovered UCC: $ucc"),
      (fd: PartialFD) => actorPrint(s"Discovered FD: $fd")
    )

    // Allocate the workers.
    val parallelism =
      if (configuration.parallelism > 0) configuration.parallelism
      else Runtime.getRuntime.availableProcessors
    workerPool = context.system.actorOf(SmallestMailboxPool(parallelism).props(Props[Worker]), "worker-pool")
    numIdleWorkers = parallelism
    actorPrint(s"Started $numIdleWorkers workers.")

    context.system.actorSelection(workerPool.path / "*") ! profilingContext

    controller ! NodeManagerState(numIdleWorkers = Some(numIdleWorkers))
  }

}

object NodeManager {

  def createOn(actorSystem: ActorSystem) = actorSystem.actorOf(Props[NodeManager], "node-manager")

}
