package de.hpi.isg.pyro.akka.actors

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorLogging, ActorRef, DeadLetter, Props, SupervisorStrategy}
import akka.routing.SmallestMailboxPool
import de.hpi.isg.pyro.akka.PyroOnAkka.{InputMethod, LocalFileInputMethod, RelationalInputGeneratorInputMethod}
import de.hpi.isg.pyro.akka.actors.Collector.{DiscoveredFD, DiscoveredUCC}
import de.hpi.isg.pyro.akka.actors.Controller._
import de.hpi.isg.pyro.akka.actors.NodeManager._
import de.hpi.isg.pyro.akka.actors.Worker.DiscoveryTask
import de.hpi.isg.pyro.akka.utils.AkkaUtils
import de.hpi.isg.pyro.akka.utils.JavaScalaCompatibility._
import de.hpi.isg.pyro.core.{Configuration, ProfilingContext, SearchSpace}
import de.hpi.isg.pyro.model.{ColumnLayoutRelationData, PartialFD, PartialKey}
import de.metanome.algorithm_integration.input.RelationalInputGenerator
import de.metanome.backend.input.file.DefaultFileInputGenerator

import scala.concurrent.duration._

import scala.collection.mutable

/**
  * There should be one such [[Actor]] on each machine. It's purpose is to control the resources and profiling process
  * on that very node.
  */
class NodeManager(controller: ActorRef,
                  configuration: Configuration,
                  input: InputMethod,
                  collector: ActorRef)
  extends Actor with ActorLogging {

  /**
    * Reference to the [[Worker]] pool under this instance.
    */
  var workerPool: ActorRef = _

  /**
    * Number of [[Worker]]s.
    */
  var numWorkers: Int = _

  /**
    * Number of idle [[Worker]]s.
    */
  var numIdleWorkers: Int = _

  /**
    * Maintains data to do the profiling upon.
    */
  var profilingContext: ProfilingContext = _

  /**
    * Maintains search spaces to be processed by this instance and keeps track of how many [[Worker]]s are processing
    * each one.
    */
  private val numAssignedWorkers: mutable.Map[SearchSpace, Int] = mutable.Map()

  /**
    * Maintains [[SearchSpace]]s that are interrupted.
    */
  private val suspendedSearchSpaces = mutable.Set[SearchSpace]()

  /**
    * Maintains [[SearchSpace]]s that [[Worker]]s dropped out from.
    */
  private val dropOutSearchSpaces = mutable.Set[SearchSpace]()

  /**
    * Counts the number of dependencies discovered.
    */
  val numDiscoveredDependencies = new AtomicInteger(0)

  override val supervisorStrategy: SupervisorStrategy = AkkaUtils.escalateSupervisorStrategy

  override def preStart(): Unit = {
    super.preStart()
    context.system.eventStream.subscribe(self, classOf[DeadLetter])
  }

  override def receive = {
    case ReportCapacity =>
      sender ! CapacityReport(capacity)

    case InitializeProfilingContext =>
      // Obtain the relation.
      val relation = input match {
        case RelationalInputGeneratorInputMethod(inputGenerator) =>
          log.debug(s"Loading relation from $inputGenerator...")
          ColumnLayoutRelationData.createFrom(inputGenerator,
            configuration.isNullEqualNull,
            configuration.maxCols,
            configuration.maxRows
          )

        case LocalFileInputMethod(inputPath, csvSettings) =>
          log.debug(s"Loading relation from $inputPath.")
          val inputGenerator = new DefaultFileInputGenerator(new File(inputPath), csvSettings)
          ColumnLayoutRelationData.createFrom(inputGenerator,
            configuration.isNullEqualNull,
            configuration.maxCols,
            configuration.maxRows
          )

        case other =>
          sys.error(s"Unsupported input method ($other).")
      }

      // Do further initializations.
      createProfilingContext(relation)
      createWorkers()

      // Pass the controller the schema.
      sender ! SchemaReport(relation.getSchema)

    case ProfilingTask(searchSpaces) =>
      searchSpaces.foreach { searchSpace =>
        require(profilingContext != null)
        searchSpace.setContext(profilingContext)
        searchSpace.ensureInitialized()
        this.numAssignedWorkers.getOrElseUpdate(searchSpace, 0)
      }
      assignSearchSpaces()

    case ReportProfilingContext =>
      sender ! ProfilingContextReport(profilingContext)

    case WorkerStopped(searchSpace) =>
      numIdleWorkers += 1
      val newAssignedWorkers = numAssignedWorkers(searchSpace) - 1
      numAssignedWorkers(searchSpace) = newAssignedWorkers

      if (newAssignedWorkers == 0) {
        if (!searchSpace.hasLaunchpads) {
          log.debug(s"$searchSpace has been completed.")
          numAssignedWorkers -= searchSpace
          controller ! SearchSpaceReport(searchSpace.id, SearchSpaceComplete)
        } else if (searchSpace.isInterruptFlagSet) {
          // TODO: Handle interruption properly.
          sys.error("TODO")
        } else {
          sys.error("Stopped working on search space for an unknown reason.")
        }

      } else {
        log.info(s"Worker dropped out from $searchSpace.")
        dropOutSearchSpaces += searchSpace
        context.system.scheduler.scheduleOnce(100 milliseconds, self, ClearDropOutSeachSpaces)(context.system.dispatcher)
      }

    case ClearDropOutSeachSpaces =>
      // TODO: Handle drop out properly.
      if (dropOutSearchSpaces.nonEmpty) {
        log.info("Clearing drop-outs.")
        dropOutSearchSpaces.clear()
        assignSearchSpaces()
      }

    case ReportNumDependencies =>
      sender() ! NodeManagerReport(numDiscoveredDependencies.get)

    case deadLetter: DeadLetter =>
      sys.error(s"Encountered $deadLetter.")

    case other => sys.error(s"[${self.path}] Unknown message: $other")
  }

  /**
    * Creates the [[ProfilingContext]] for this instance.
    *
    * @param relation the relation to be profiled
    */
  private def createProfilingContext(relation: ColumnLayoutRelationData) = {
    profilingContext = new ProfilingContext(
      configuration,
      relation,
      (ucc: PartialKey) => {
        collector ! DiscoveredUCC(ucc)
        numDiscoveredDependencies.incrementAndGet()
      },
      (fd: PartialFD) => {
        collector ! DiscoveredFD(fd)
        numDiscoveredDependencies.incrementAndGet()
      }
    )
  }

  /**
    * Creates the [[Worker]] actor pool and notifies the [[controller]] of the new state.
    */
  def createWorkers() {
    // Allocate the workers.
    numWorkers = capacity
    workerPool = context.actorOf(SmallestMailboxPool(numWorkers).props(Worker.props(profilingContext)), "worker-pool")
    numIdleWorkers = numWorkers
    log.info(s"Started $numWorkers workers.")
  }

  /**
    * Determine how many [[Worker]]s this instance should have according to the [[configuration]] and resources.
    *
    * @return the number of [[Worker]]s
    */
  private def capacity =
    if (configuration.parallelism > 0) configuration.parallelism
    else Runtime.getRuntime.availableProcessors

  /**
    * Assign [[SearchSpace]]s to idling [[Worker]]s.
    */
  private def assignSearchSpaces(): Unit = {
    // Naive implementation. As long as there are idling workers, we just assign them whatever search space has the
    // fewest workers operating upon it.
    implicit val searchSpaceOrdering = Ordering.by[(SearchSpace, Int), Int](_._2)(Ordering.Int.reverse)
    val searchSpaceQueue = this.numAssignedWorkers.filter { case (searchSpace, assigned) =>
       checkAdmissionForAdditionalWorker(assigned) && !suspendedSearchSpaces.contains(searchSpace)
    }.to[mutable.PriorityQueue]

    while (numIdleWorkers > 0 && searchSpaceQueue.nonEmpty) {
      val (searchSpace, numWorkingWorkers) = searchSpaceQueue.dequeue()
      searchSpace.setContext(profilingContext)
      log.debug(s"Assigning $searchSpace to a worker (processed by $numWorkingWorkers other workers)")
      workerPool ! DiscoveryTask(searchSpace)
      numIdleWorkers -= 1
      numAssignedWorkers(searchSpace) = numAssignedWorkers(searchSpace) + 1
      if (checkAdmissionForAdditionalWorker(numWorkingWorkers + 1))
        searchSpaceQueue.enqueue((searchSpace, numWorkingWorkers + 1))
    }

    // TODO: Revoke search spaces from workers.
  }

  private def checkAdmissionForAdditionalWorker(searchSpace: SearchSpace): Boolean =
    checkAdmissionForAdditionalWorker(numAssignedWorkers(searchSpace))

  private def checkAdmissionForAdditionalWorker(numWorkers: Int): Boolean =
    configuration.maxThreadsPerSearchSpace < 1 || configuration.maxThreadsPerSearchSpace > numWorkers

}

/**
  * Companion object.
  */
object NodeManager {

  /**
    * Creates a [[Props]] instance for a new [[NodeManager]] actor.
    *
    * @param controller    that controls the new actor
    * @param configuration that defines what to profile and how
    * @param input         defines what to profile
    * @param collector     to which discovered dependencies should be sent
    * @return the [[Props]]
    */
  def props(controller: ActorRef,
            configuration: Configuration,
            input: InputMethod,
            collector: ActorRef) =
    Props(new NodeManager(controller, configuration, input, collector))

  /**
    * This messages asks how many [[Worker]]s a [[NodeManager]] can provide.
    */
  case object ReportCapacity

  /**
    * This message asks a [[NodeManager]] actor to initialize its [[ProfilingContext]] and [[Worker]]s using the
    * [[RelationalInputGenerator]].
    */
  case object InitializeProfilingContext

  /**
    * This message asks a [[NodeManager]] to report its [[ProfilingContext]].
    */
  case object ReportProfilingContext

  /**
    * This message asks a [[NodeManager]] to profile the given [[SearchSpace]]s.
    *
    * @param searchSpaces the [[SearchSpace]]s
    */
  case class ProfilingTask(searchSpaces: Iterable[SearchSpace])

  /**
    * This message asks a [[NodeManager]] to report the number of dependencies its [[Worker]]s discovered.
    */
  case object ReportNumDependencies

  /**
    * This message tells a [[NodeManager]] to clear its drop-out [[SearchSpace]]s.
    */
  case object ClearDropOutSeachSpaces


  /**
    * This message tells that some [[Worker]] stopped processing the given [[SearchSpace]].
    *
    * @param searchSpace the [[SearchSpace]]
    */
  case class WorkerStopped(searchSpace: SearchSpace)

}
