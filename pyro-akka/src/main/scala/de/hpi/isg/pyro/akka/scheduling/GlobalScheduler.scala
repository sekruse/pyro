package de.hpi.isg.pyro.akka.scheduling

import akka.actor.ActorRef
import de.hpi.isg.pyro.akka.actors.Controller
import de.hpi.isg.pyro.akka.actors.NodeManager.ProfilingTask
import de.hpi.isg.pyro.core.SearchSpace

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * This class is keeping track of the cluster state and provides scheduling methods.
  */
class GlobalScheduler(controller: Controller) {

  private val log = controller.log

  private val nodeManagerStates = mutable.Map[ActorRef, NodeManagerState]()

  private val searchSpaceStates = mutable.Map[Int, SearchSpaceState]()

  private val completedSearchSpaceIds = mutable.Set[Int]()

  def registerNodeManager(nodeManager: ActorRef, numWorkers: Int): Unit = {
    require(!nodeManagerStates.keys.exists(_ == nodeManager))
    nodeManagerStates(nodeManager) = new NodeManagerState(nodeManager, numWorkers)
  }

  def activateNodeManager(nodeManager: ActorRef): Unit = {
    require(nodeManagerStates.keys.exists(_ == nodeManager))
    require(!nodeManagerStates(nodeManager).isInitialized)
    nodeManagerStates(nodeManager).isInitialized = true
  }

  def registerSearchSpace(searchSpace: SearchSpace): Unit = {
    require(!searchSpaceStates.keys.exists(_ == searchSpace.id))
    searchSpaceStates(searchSpace.id) = new SearchSpaceState(searchSpace)
  }

  def nodeManagers = nodeManagerStates.keys

  def searchSpace(id: Int): SearchSpace = searchSpaceStates(id).searchSpace

  def unassignedSearchSpaceStates: Iterable[SearchSpaceState] =
    searchSpaceStates.values.filter { state =>
      state.assignedNodeManagers.isEmpty && !completedSearchSpaceIds(state.searchSpaceId)
    }

  def underloadedNodeManagerStates: Iterable[NodeManagerState] =
    nodeManagerStates.values.filter(state => state.isInitialized && state.load < 0)

  def assignSearchSpaces(): Unit = {
    // Collect the unassigned search spaces.
    var unassignedSearchSpaceStates = this.unassignedSearchSpaceStates.to[mutable.Queue]

    // Create a priority queue of available nodes.
    implicit val nodeManagerQueueOrdering = Ordering.by((state: NodeManagerState) => state.load)
    val nodeQueue = underloadedNodeManagerStates.to[mutable.PriorityQueue]

    // Collect the search spaces first and then assign them in a batch.
    val assignments = mutable.Map[ActorRef, ArrayBuffer[SearchSpace]]()

    // Distribute as many search spaces as possible.
    while (unassignedSearchSpaceStates.nonEmpty && nodeQueue.nonEmpty) {
      // Get a search space.
      val searchSpaceState = unassignedSearchSpaceStates.dequeue()
      val searchSpace = searchSpaceState.searchSpace

      // Get a node.
      val nodeManagerState = nodeQueue.dequeue()
      val nodeManager = nodeManagerState.nodeManager

      // Assign the search space to the node.
      log.info(s"Assigning $searchSpace to $nodeManager (load: ${nodeManagerState.load})")
      assignments.getOrElseUpdate(nodeManager, ArrayBuffer()) += searchSpace

      // Update the states.
      searchSpaceState.assignedNodeManagers += nodeManager
      nodeManagerState.assignedSearchSpaceIds += searchSpace.id
      if (nodeManagerState.load < 0) nodeQueue += nodeManagerState
    }

    // TODO: Do this in a synchronous fashion?
    // Finally, dispatch the assignments.
    assignments.foreach {
      case (node, assignedSearchSpaces) => node ! ProfilingTask(assignedSearchSpaces)
    }
  }

  def handleSearchSpaceCompleted(nodeManager: ActorRef, searchSpaceId: Int): Unit = {
    val searchSpaceState = searchSpaceStates(searchSpaceId)
    assert(searchSpaceState.assignedNodeManagers.contains(nodeManager))

    // Disassociate the node manager and search space.
    nodeManagerStates(nodeManager).assignedSearchSpaceIds -= searchSpaceId
    searchSpaceState.assignedNodeManagers -= nodeManager

    if (searchSpaceState.assignedNodeManagers.isEmpty) {
      // If the search space was assigned to only a single node, then we can directly remove it.
      searchSpaceStates -= searchSpaceId
      completedSearchSpaceIds -= searchSpaceId

    } else {
      // If the search space was assigned to more than one node, then we need to keep track of it.
      completedSearchSpaceIds += searchSpaceId
    }
  }

  def isComplete = searchSpaceStates.isEmpty

}

/** This class keeps track of the state of a [[de.hpi.isg.pyro.akka.actors.NodeManager]]. */
class NodeManagerState(val nodeManager: ActorRef, val numWorkers: Int) {

  /** Whether the [[de.hpi.isg.pyro.akka.actors.NodeManager]] is initialized and can start profiling. */
  var isInitialized = false

  /** IDs of the [[de.hpi.isg.pyro.core.SearchSpace]]s assigned to the [[de.hpi.isg.pyro.akka.actors.NodeManager]]. */
  val assignedSearchSpaceIds: mutable.Set[Int] = mutable.Set()

  def load = assignedSearchSpaceIds.size - numWorkers

}

/** This class keeps track of the state of a [[de.hpi.isg.pyro.core.SearchSpace]]. */
class SearchSpaceState private(val searchSpaceId: Int) {

  def this(searchSpace: SearchSpace) = {
    this(searchSpace.id)
    this.searchSpace = searchSpace
  }

  var searchSpace: SearchSpace = _

  /** [[akka.actor.ActorRef]]s that are assigned the [[de.hpi.isg.pyro.core.SearchSpace]]. */
  val assignedNodeManagers: mutable.Set[ActorRef] = mutable.Set()

}