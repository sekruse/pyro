package de.hpi.isg.pyro.akka.actors

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Address, Deploy, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy}
import akka.remote.RemoteScope
import akka.util.Timeout
import de.hpi.isg.profiledb.store.model.Experiment
import de.hpi.isg.pyro.akka.actors.Collector.{InitializeCollector, SignalWhenDone}
import de.hpi.isg.pyro.akka.actors.NodeManager._
import de.hpi.isg.pyro.akka.algorithms.Pyro.{InputMethod, OutputMethod}
import de.hpi.isg.pyro.akka.scheduling.GlobalScheduler
import de.hpi.isg.pyro.akka.utils.{AskingMany, Host}
import de.hpi.isg.pyro.core._
import de.hpi.isg.pyro.model.RelationSchema
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * The purpose of this [[Actor]] is to steer the basic execution of Pyro.
  *
  * @param configuration keeps track of the [[Configuration]] for the profiling
  */
class Controller(configuration: Configuration,
                 input: InputMethod,
                 output: OutputMethod,
                 master: Host,
                 workers: Array[Host] = Array(),
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
    * This variable is set once we obtain it. It should then not be changed anymore.
    */
  private var schema: RelationSchema = _

  /**
    * Takes care of the scheduling of profiling subtasks.
    */
  private val scheduler = new GlobalScheduler(this)

  /**
    * The local [[NodeManager]] actor, i.e., the one that is on the same machine.
    */
  private var localNodeManager: ActorRef = _

  /**
    * [[ActorRef]] to the [[Collector]].
    */
  private var collector: ActorRef = _


  override def preStart(): Unit = {
    super.preStart()

    // Initialize the Collector actor.
    collector = context.actorOf(Collector.props(output.fdConsumer, output.uccConsumer), "collector")

    // Initialize NodeManagers.
    val nodeManagerProps = NodeManager.props(self, configuration, input, collector)
    val nodeManagers: Iterable[ActorRef] =
      if (workers.isEmpty) {
        // Create a local node manager only.
        log.info("Creating a local node manager...")
        Iterable(createNodeManager(nodeManagerProps))
      } else {
        // Create remote node managers.
        workers.zipWithIndex.map {
          case (host, index) => createNodeManager(nodeManagerProps, Some(host), Some(index))
        }
      }

    log.debug("Awaiting node capacity reports...")
    askAll[CapacityReport](nodeManagers, ReportCapacity) foreach {
      case (nodeManager, CapacityReport(capacity)) =>
        scheduler.registerNodeManager(nodeManager, capacity)
        log.debug(s"$nodeManager reported a capacity of $capacity.")
    }
  }

  /**
    * Create a new [[NodeManager]] actor. If a local actor is created, it will be stored to [[localNodeManager]].
    *
    * @param props the [[NodeManager]] initialization properties
    * @param host  an optional [[Host]] to create the actor on; if [[None]], then a local actor will be created
    * @param index an optional index for the name of the new actor
    * @return an [[ActorRef]] to the created actor
    */
  private def createNodeManager(props: Props, host: Option[Host] = None, index: Option[Int] = None): ActorRef = {
    val isCreateLocal = host match {
      case Some(`master`) => true
      case None => true
      case _ => false
    }
    val name = index match {
      case Some(i) => f"nodemgr-$i%02d"
      case None => "nodemgr"
    }
    if (isCreateLocal) {
      log.info("Creating a local node manager...")
      localNodeManager = context.actorOf(props, name)
      localNodeManager
    } else {
      val Host(hostName, port) = host.get
      log.info(s"Creating a remote node manager at $hostName:$port...")
      val deploy = new Deploy(RemoteScope(new Address("akka.tcp", "pyro", hostName, port)))
      val remoteNodeManager = context.actorOf(props.withDeploy(deploy), name)
      remoteNodeManager
    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    case Start =>
      log.debug("Received start message.")
      log.debug("Initializing profiling context on nodes...")
      scheduler.nodeManagers foreach {
        _ ! InitializeProfilingContext
      }

    case SchemaReport(relationSchema) =>
      schema match {
        case null =>
          schema = relationSchema
          collector ! InitializeCollector(schema)
          initializeSearchSpaces(schema)
        case _ =>
      }
      scheduler activateNodeManager sender
      assignSearchSpaces()

    case SearchSpaceReport(searchSpaceId, SearchSpaceComplete) =>
      log.debug(s"Received search space report from $sender.")
      log.info(s"${scheduler.searchSpace(searchSpaceId)} is complete.")
      scheduler.handleSearchSpaceCompleted(sender, searchSpaceId)
      if (scheduler.isComplete) signalCollectorToComplete()
      else assignSearchSpaces()

    case CollectorComplete =>
      log.debug(s"Collector has completed.")
      onSuccess()
      context.system.terminate()

    case other =>
      sys.error(s"[${self.path}] Cannot handle $other")
  }

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: Throwable =>
      log.error(e, "Exception encountered.")
      log.info("Shutting down due to exception.")
      scheduler.nodeManagers foreach {
        _ ! PoisonPill
      }
      context.system.terminate()
      Escalate
  }

  /**
    * Initialize the [[SearchSpace]]s.
    *
    * @param schema of the relation to be profiled
    */
  private def initializeSearchSpaces(schema: RelationSchema): Unit = {
    val nextId = {
      var i = -1
      () => {
        i += 1
        i
      }
    }
    // Initialize the UCC search space.
    if (configuration.isFindKeys) {
      configuration.uccErrorMeasure match {
        case "g1prime" =>
          val strategy = new KeyG1Strategy(configuration.maxUccError)
          scheduler.registerSearchSpace(new SearchSpace(nextId(), strategy, schema))
        case other => sys.error(s"Unsupported error measure ($other).")
      }

    }
    // Initialize the FD search spaces.
    if (configuration.isFindFds) {
      schema.getColumns foreach { column =>
        configuration.fdErrorMeasure match {
          case "g1prime" =>
            val strategy = new FdG1Strategy(column, configuration.maxFdError)
            scheduler.registerSearchSpace(new SearchSpace(nextId(), strategy, schema))
        }
      }
    }
  }

  /**
    * Let the [[scheduler]] re-assign [[SearchSpace]]s to [[NodeManager]]s.
    */
  private def assignSearchSpaces(): Unit = {
    scheduler.assignSearchSpaces()
  }

  /**
    * Asks all [[NodeManager]]s to report how many dependencies discovered and tell the [[Collector]] to signal when
    * this number of dependencies has been collected.
    */
  private def signalCollectorToComplete(): Unit = {
    val numDependencies = askAll[NodeManagerReport](scheduler.nodeManagers, ReportNumDependencies).values
      .map(_.numDiscoveredDependencies)
      .sum
    log.info("Workers reported {} dependencies.", numDependencies)
    collector ! SignalWhenDone(numDependencies)
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
            master: Host,
            workers: Array[Host] = Array(),
            onSuccess: () => Unit,
            experiment: Option[Experiment] = None) = {

    // Initialize the controller.
    // TODO: Pass experiment.
    val controller = actorSystem.actorOf(
      Props(classOf[Controller], configuration, input, output, master, workers, onSuccess),
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
    * This message informs the [[Controller]] of the dataset's schema.
    *
    * @param schema the [[RelationSchema]]
    */
  case class SchemaReport(schema: RelationSchema)


  //  /**
  //    * This message communicates the state of a [[de.hpi.isg.pyro.akka.actors.NodeManager]].
  //    *
  //    * @param numWorkers      number of idle [[de.hpi.isg.pyro.akka.actors.Worker]]s
  //    * @param numSearchSpaces number of search spaces being processed by a node
  //    */
  //  case class NodeManagerState(numWorkers: Int, numSearchSpaces: Int) {
  //
  //    /**
  //      * Increases the number of search spaces.
  //      *
  //      * @param numAdditionalSearchSpaces the number of additional search spaces.
  //      * @return a new instance
  //      */
  //    def +(numAdditionalSearchSpaces: Int) = NodeManagerState(numWorkers, numSearchSpaces + numAdditionalSearchSpaces)
  //
  //
  //    /**
  //      * Defines the load of a node.
  //      *
  //      * @return [[numSearchSpaces]] - [[numWorkers]]
  //      */
  //    def load: Int = numSearchSpaces - numWorkers
  //
  //  }

  /**
    * This message passes a [[ProfilingContext]]. This message should only be passed locally.
    *
    * @param profilingContext the [[ProfilingContext]]
    */
  case class ProfilingContextReport(profilingContext: ProfilingContext)

  //  /**
  //    * Orders [[NodeManagerState]]s ascending by their load (`workers - assigned search spaces`).
  //    */
  //  implicit val nodeManagerLoadOrdering: Ordering[NodeManagerState] =
  //    Ordering.by((state: NodeManagerState) => state.numWorkers - state.numSearchSpaces)(Ordering.Int)

  /**
    * This message reports the capacity of a [[NodeManager]].
    *
    * @param capacity the capacity
    */
  case class CapacityReport(capacity: Int)

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
    * Describes that processing of a [[SearchSpace]] is complete.
    */
  case object SearchSpaceStopped extends SearchSpaceReportState

  /**
    * This message signals that the [[Collector]] has collected all dependencies.
    */
  case object CollectorComplete

}