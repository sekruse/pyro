package de.hpi.isg.pyro.akka.actors

import akka.actor.{Actor, ActorRef, ActorSystem, Address, Deploy, Props}
import akka.remote.RemoteScope
import de.hpi.isg.pyro.akka.messages.{ColumnReport, NodeManagerInitialization, NodeManagerState, ProfilingTask}
import de.hpi.isg.pyro.akka.utils.Host
import de.hpi.isg.pyro.core.{Configuration, FdG1Strategy, KeyG1Strategy, SearchSpace}
import de.hpi.isg.pyro.model.Column
import de.metanome.algorithm_integration.input.RelationalInputGenerator
import de.metanome.backend.result_receiver.ResultReceiver
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * The purpose of this [[Actor]] is to steer the basic execution of Pyro.
  */
class Controller extends Actor with Printing {

  /**
    * Logger for this instance.
    */
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Keeps track of the [[Configuration]] for the profiling.
    */
  var configuration: Configuration = _

  /**
    * References to the controlled [[NodeManager]]s. Additionally, keeps track of how many idle [[Worker]]s there
    * are per [[NodeManager]].
    */
  var numIdleWorkersPerNodeManager: mutable.Map[ActorRef, Int] = mutable.Map()

  /**
    * Keeps track of all uncompleted [[SearchSpace]]s including pointers to which [[NodeManager]]s are currently
    * processing them.
    */
  var searchSpaces: mutable.Map[SearchSpace, mutable.Set[ActorRef]] = _

  override def receive = {
    case NodeManagerInitialization(hosts) =>
      if (hosts.isEmpty) {
        // Create a local node manager only.
        actorPrint("Creating a local node manager...")
        val localNodeManager = context.actorOf(Props[NodeManager])
        numIdleWorkersPerNodeManager(localNodeManager) = 0
      } else {
        // Create remote node managers.
        hosts.foreach { case Host(host, port) =>
          actorPrint(s"Creating a remote node manager at $host:$port...")
          val deploy = new Deploy(RemoteScope(new Address("akka.tcp", "pyro", host, port)))
          val remoteNodeManager = context.actorOf(Props[NodeManager].withDeploy(deploy))
          numIdleWorkersPerNodeManager(remoteNodeManager) = 0
        }
      }

    case NodeManagerState(numIdleWorkers) =>
      numIdleWorkers match {
        case Some(num) =>
          numIdleWorkersPerNodeManager(sender()) = num
          if (searchSpaces != null) assignSearchSpaces()
      }

    case ProfilingTask(conf, inputPath, inputGenerator) =>
      // Start the node managers.
      numIdleWorkersPerNodeManager.keysIterator.foreach(_ ! ProfilingTask(conf, inputPath, inputGenerator))
      configuration = conf

    case ColumnReport(columns) =>
      if (searchSpaces == null) {
        // Only handle the message if we have not yet initialized the search spaces.
        initializeSearchSpaces(columns)
        assignSearchSpaces()
      }

    case other =>
      sys.error(s"[${self.path}] Cannot handle $other")
  }

  /**
    * Initialize the [[searchSpaces]].
    *
    * @param columns of the relation to be profiled
    */
  private def initializeSearchSpaces(columns: Seq[Column]): Unit = {
    searchSpaces = mutable.Map()
    // Initialize the UCC search space.
    if (configuration.isFindKeys) {
      configuration.uccErrorMeasure match {
        case "g1prime" => searchSpaces(new SearchSpace(new KeyG1Strategy(configuration.maxUccError))) = mutable.Set()
        case other => sys.error(s"Unsupported error measure ($other).")
      }

    }
    // Initialize the FD search spaces.
    if (configuration.isFindFds) {
      columns foreach { column =>
        configuration.fdErrorMeasure match {
          case "g1prime" =>
            val strategy = new FdG1Strategy(column, configuration.maxFdError)
            // TODO: Check 0-ary FD at some worker.
            searchSpaces(new SearchSpace(strategy)) = mutable.Set()
        }
      }
    }
  }

  /**
    * Assign unprocessed [[SearchSpace]]s in [[searchSpaces]] to [[NodeManager]]s with idling [[Worker]]s (as
    * constituted in [[numIdleWorkersPerNodeManager]]).
    */
  private def assignSearchSpaces(): Unit = {
    // Collect the unassigned search spaces.
    var unassignedSearchSpaces = searchSpaces.filter(_._2.isEmpty).keys.toList

    // Create a priority queue of available nodes.
    val nodeQueue = mutable.PriorityQueue[(ActorRef, Int)]()(Ordering.by(-_._2))
    numIdleWorkersPerNodeManager.filter(_._2 > 0).foreach(nodeQueue += _)

    // Distribute as many search spaces as possible.
    while (unassignedSearchSpaces.nonEmpty && nodeQueue.nonEmpty) {
      // Get a search space.
      val searchSpace = unassignedSearchSpaces.head
      unassignedSearchSpaces = unassignedSearchSpaces.tail

      // Get a node.
      val (node, numIdleWorkers) = nodeQueue.dequeue()

      // Assign the search space to the node.
      actorPrint(s"Assigning $searchSpace to $node (has $numIdleWorkers idle workers)")
      node ! searchSpace

      // Update the data structures.
      val newNumIdleWorkers = numIdleWorkers - 1
      numIdleWorkersPerNodeManager(node) = newNumIdleWorkers
      if (newNumIdleWorkers > 0) nodeQueue += node -> newNumIdleWorkers
    }
  }

}

/**
  * Utilities to work with [[Controller]]s.
  */
object Controller {

  /**
    * Sets up a [[Controller]] in the [[ActorSystem]] and starts it.
    *
    * @param actorSystem   the [[ActorSystem]]
    * @param configuration the [[Configuration]] of what to profile and how
    */
  def start(actorSystem: ActorSystem,
            configuration: Configuration,
            inputPath: String,
            inputGenerator: Option[RelationalInputGenerator] = None,
            resultReceiver: Option[ResultReceiver] = None,
            hosts: Array[Host] = Array()) = {
    // Initialize the controller.
    val controller = actorSystem.actorOf(Props[Controller], "controller")

    // Initialize the node managers.
    controller ! NodeManagerInitialization(hosts)

    // Initiate the profiling task.
    controller ! ProfilingTask(configuration, inputPath, inputGenerator)
  }

}