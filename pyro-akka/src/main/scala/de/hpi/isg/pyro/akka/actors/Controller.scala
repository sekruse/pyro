package de.hpi.isg.pyro.akka.actors

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Address, Deploy, OneForOneStrategy, Props, SupervisorStrategy}
import akka.pattern.ask
import akka.remote.RemoteScope
import akka.util.Timeout
import de.hpi.isg.pyro.akka.PyroOnAkka.{InputMethod, OutputMethod}
import de.hpi.isg.pyro.akka.actors.Collector.{InitializeCollector, SignalWhenDone}
import de.hpi.isg.pyro.akka.actors.NodeManager.{InitializeProfilingContext, ProfilingTask, ReportNumDependencies, ReportProfilingContext}
import de.hpi.isg.pyro.akka.utils.{AskingMany, Host}
import de.hpi.isg.pyro.core._
import de.hpi.isg.pyro.model.RelationSchema
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
  * The purpose of this [[Actor]] is to steer the basic execution of Pyro.
  *
  * @param configuration keeps track of the [[Configuration]] for the profiling
  */
class Controller(configuration: Configuration,
                 input: InputMethod,
                 output: OutputMethod,
                 hosts: Array[Host] = Array(),
                 onSuccess: () => Unit)
  extends Actor with ActorLogging with AskingMany {

  import Controller._

  /**
    * Provides an implicit [[Timeout]] value.
    *
    * @return the [[Timeout]]
    */
  implicit def timeout = Timeout(42 days)

  implicit var profilingContext: ProfilingContext = _

  /**
    * Logger for this instance.
    */
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * References to the controlled [[NodeManager]]s. Additionally, keeps track of how many idle [[Worker]]s there
    * are per [[NodeManager]].
    */
  private val nodeManagerStates: mutable.Map[ActorRef, NodeManagerState] = mutable.Map()

  /**
    * The local [[NodeManager]] actor, i.e., the one that is on the same machine.
    */
  private var localNodeManager: ActorRef = _

  /**
    * [[ActorRef]] to the [[Collector]].
    */
  private var collector: ActorRef = _

  /**
    * Keeps track of all uncompleted [[SearchSpace]]s including pointers to which [[NodeManager]]s are currently
    * processing them.
    */
  var searchSpaces: mutable.Map[SearchSpace, mutable.Set[ActorRef]] = _


  override def preStart(): Unit = {
    super.preStart()

    // Initialize the Collector actor.
    collector = context.actorOf(Collector.props(output.fdConsumer, output.uccConsumer), "collector")

    // Initialize NodeManagers.
    val nodeManagerProps = NodeManager.props(self, configuration, input, collector)
    if (hosts.isEmpty) {
      // Create a local node manager only.
      log.info("Creating a local node manager...")
      localNodeManager = context.actorOf(nodeManagerProps)
      nodeManagerStates(localNodeManager) = NodeManagerState(numWorkers = 0, numSearchSpaces = 0)
    } else {
      // Create remote node managers.
      hosts.foreach { case Host(host, port) =>
        log.info(s"Creating a remote node manager at $host:$port...")
        val deploy = new Deploy(RemoteScope(new Address("akka.tcp", "pyro", host, port)))
        val remoteNodeManager = context.actorOf(nodeManagerProps.withDeploy(deploy))
        if (localNodeManager == null) localNodeManager = remoteNodeManager // By convention, the first host must be local.
        nodeManagerStates(remoteNodeManager) = NodeManagerState(numWorkers = 0, numSearchSpaces = 0)
      }
    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    case Start =>
      askAll[NodeManagerState](nodeManagerStates.keys, InitializeProfilingContext) foreach {
        case (node, state) => nodeManagerStates(node) = state
      }
      implicit val executionContext = context.system.dispatcher
      (localNodeManager ? ReportProfilingContext).mapTo[ProfilingContextReport] onComplete {
        case Success(ProfilingContextReport(ctx)) =>
          // Initialize the Collector actor.
          profilingContext = ctx
          collector ! InitializeCollector(profilingContext)

        case Failure(e) => throw e
      }

    case schema: RelationSchema =>
      if (searchSpaces == null) {
        // Only handle the message if we have not yet initialized the search spaces.
        initializeSearchSpaces(schema)
        assignSearchSpaces()
      }

    case nodeManagerState: NodeManagerState =>
      nodeManagerStates(sender()) = nodeManagerState
      if (searchSpaces != null) {
        assignSearchSpaces()
        if (searchSpaces.isEmpty && nodeManagerStates.valuesIterator.forall {
          case NodeManagerState(_, numSearchSpaces) => numSearchSpaces == 0
        }) {
          val numDependencies = askAll[NodeManagerReport](nodeManagerStates.keys, ReportNumDependencies).values
            .map(_.numDiscoveredDependencies)
            .sum
          log.info("Workers reported {} dependencies.", numDependencies)
          collector ! SignalWhenDone(numDependencies)
        }
      }

    case SearchSpaceReport(searchSpaceId, SearchSpaceComplete) =>
      Try {
        val searchSpace = searchSpaces.keysIterator.find(_.id == searchSpaceId).getOrElse(throw new IllegalArgumentException)
        log.info(s"$searchSpace has been completed by ${sender()}.")
        searchSpaces -= searchSpace
      } match {
        case Success(_) =>
          log.info(s"Sucessfully removed search space with ID $searchSpaceId.")
        case Failure(throwable) =>
          log.error(s"Could not remove search space with ID $searchSpaceId from $searchSpaces.", throwable)
      }

    case CollectorComplete =>
      onSuccess()
      context.system.terminate()

    case other =>
      sys.error(s"[${self.path}] Cannot handle $other")
  }

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: Throwable =>
      log.error(e, "Exception encountered.")
      log.info("Shutting down due to exception.")
      context.system.terminate()
      Escalate
  }

  /**
    * Initialize the [[searchSpaces]].
    *
    * @param schema of the relation to be profiled
    */
  private def initializeSearchSpaces(schema: RelationSchema): Unit = {
    searchSpaces = mutable.Map()
    val nextId = {
      var i = -1;
      () => {
        i += 1;
        i
      }
    }
    // Initialize the UCC search space.
    if (configuration.isFindKeys) {
      configuration.uccErrorMeasure match {
        case "g1prime" => searchSpaces(new SearchSpace(nextId(), new KeyG1Strategy(configuration.maxUccError), schema)) = mutable.Set()
        case other => sys.error(s"Unsupported error measure ($other).")
      }

    }
    // Initialize the FD search spaces.
    if (configuration.isFindFds) {
      schema.getColumns foreach { column =>
        configuration.fdErrorMeasure match {
          case "g1prime" =>
            val strategy = new FdG1Strategy(column, configuration.maxFdError)
            searchSpaces(new SearchSpace(nextId(), strategy, schema)) = mutable.Set()
        }
      }
    }
  }

  /**
    * Assign unprocessed [[SearchSpace]]s in [[searchSpaces]] to [[NodeManager]]s with idling [[Worker]]s (as
    * constituted in [[nodeManagerStates]]).
    */
  private def assignSearchSpaces(): Unit = {
    // Collect the unassigned search spaces.
    var unassignedSearchSpaces = searchSpaces.filter(_._2.isEmpty).keys.toList

    // Create a priority queue of available nodes.
    val nodeQueue = mutable.PriorityQueue[(ActorRef, NodeManagerState)]()
    nodeManagerStates.filter(_._2.load < 0).foreach(nodeQueue += _)

    // Collect the search spaces first and then assign them in a batch.
    val assignments = mutable.Map[ActorRef, ArrayBuffer[SearchSpace]]()

    // Distribute as many search spaces as possible.
    while (unassignedSearchSpaces.nonEmpty && nodeQueue.nonEmpty) {
      // Get a search space.
      val searchSpace = unassignedSearchSpaces.head
      unassignedSearchSpaces = unassignedSearchSpaces.tail

      // Get a node.
      val (node, state) = nodeQueue.dequeue()

      // Assign the search space to the node.
      log.debug(s"Assigning $searchSpace to $node (load: ${state.load})")
      assignments.getOrElseUpdate(node, ArrayBuffer()) += searchSpace

      // Update the data structures.
      val newState = state + 1
      nodeManagerStates(node) = newState
      if (newState.load < 0) nodeQueue += node -> newState
    }

    // TODO: Do this in a synchonous fashion?
    // Finally, dispatch the assignments.
    assignments.foreach {
      case (node, assignedSearchSpaces) => node ! ProfilingTask(assignedSearchSpaces)
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
            input: InputMethod,
            output: OutputMethod,
            hosts: Array[Host] = Array(),
            onSuccess: () => Unit) = {

    // Initialize the controller.
    val controller = actorSystem.actorOf(
      Props(classOf[Controller], configuration, input, output, hosts, onSuccess),
      "controller"
    )

    // Initiate the profiling task.
    controller ! Start
  }

  /**
    * Message to trigger the profiling.
    */
  case object Start

  /**
    * This message communicates the state of a [[de.hpi.isg.pyro.akka.actors.NodeManager]].
    *
    * @param numWorkers      number of idle [[de.hpi.isg.pyro.akka.actors.Worker]]s
    * @param numSearchSpaces number of search spaces being processed by a node
    */
  case class NodeManagerState(numWorkers: Int, numSearchSpaces: Int) {

    /**
      * Increases the number of search spaces.
      *
      * @param numAdditionalSearchSpaces the number of additional search spaces.
      * @return a new instance
      */
    def +(numAdditionalSearchSpaces: Int) = NodeManagerState(numWorkers, numSearchSpaces + numAdditionalSearchSpaces)


    /**
      * Defines the load of a node.
      *
      * @return [[numSearchSpaces]] - [[numWorkers]]
      */
    def load: Int = numSearchSpaces - numWorkers

  }

  /**
    * This message passes a [[ProfilingContext]]. This message should only be passed locally.
    *
    * @param profilingContext the [[ProfilingContext]]
    */
  case class ProfilingContextReport(profilingContext: ProfilingContext)

  /**
    * Orders [[NodeManagerState]]s ascending by their load (`workers - assigned search spaces`).
    */
  implicit val nodeManagerLoadOrdering: Ordering[NodeManagerState] =
    Ordering.by((state: NodeManagerState) => state.numWorkers - state.numSearchSpaces)(Ordering.Int)

  /**
    * This message is the terminal report of a [[NodeManager]] that tells how many dependencies were discovered on the
    * respective node.
    *
    * @param numDiscoveredDependencies the number of discovered dependencies
    */
  case class NodeManagerReport(numDiscoveredDependencies: Int)

  /**
    * Describes the advancement of the processing of some [[SearchSpace]].
    *
    * @param searchSpaceId the ID of the [[SearchSpace]]
    */
  case class SearchSpaceReport(searchSpaceId: Int, state: SearchSpaceReportState)

  /**
    * Describes a state for the [[SearchSpaceReport]].
    */
  sealed trait SearchSpaceReportState

  /**
    * Describes that processing of a [[SearchSpace]] is complete.
    */
  case object SearchSpaceComplete extends SearchSpaceReportState

  /**
    * This message signals that the [[Collector]] has collected all dependencies.
    */
  case object CollectorComplete

}