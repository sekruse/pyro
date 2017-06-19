package de.hpi.isg.pyro.akka.actors

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{Actor, ActorLogging, ActorRef, DeadLetter, OneForOneStrategy, Props, SupervisorStrategy}
import akka.routing.SmallestMailboxPool
import de.hpi.isg.pyro.akka.actors.Controller.{NodeManagerReport, NodeManagerState, SearchSpaceComplete, SearchSpaceReport}
import de.hpi.isg.pyro.akka.actors.NodeManager.{InitializeFromInputGenerator, ReportNumDependencies, WorkerStopped}
import de.hpi.isg.pyro.akka.utils.AkkaUtils
import de.hpi.isg.pyro.akka.utils.JavaScalaCompatibility._
import de.hpi.isg.pyro.core.{Configuration, ProfilingContext, SearchSpace}
import de.hpi.isg.pyro.model.{ColumnLayoutRelationData, PartialFD, PartialKey}
import de.metanome.algorithm_integration.input.RelationalInputGenerator

import scala.collection.mutable

/**
  * There should be one such [[Actor]] on each machine. It's purpose is to control the resources and profiling process
  * on that very node.
  */
class NodeManager(controller: ActorRef,
                  configuration: Configuration,
                  inputPath: String,
                  inputGenerator: Option[RelationalInputGenerator],
                  uccConsumer: PartialKey => _,
                  fdConsumer: PartialFD => _)
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
  var numAssignedWorkers: mutable.Map[SearchSpace, Int] = mutable.Map()

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
    case InitializeFromInputGenerator =>
      // Obtain the relation.
      val relation = inputGenerator match {
        case Some(generator) =>
          log.info(s"Loading relation from $generator...")
          ColumnLayoutRelationData.createFrom(generator,
            configuration.isNullEqualNull,
            configuration.maxCols,
            configuration.maxRows
          )

        case None =>
          sys.error(s"No RelationalInputGenerator not supported.")
      }

      // Do further initializations.
      createProfilingContext(relation)
      createWorkers()

      // Pass the controller the schema.
      controller ! relation.getSchema

    case searchSpaces: Seq[SearchSpace] =>
      searchSpaces.foreach { searchSpace =>
        require(profilingContext != null)
        searchSpace.setContext(profilingContext)
        searchSpace.ensureInitialized()
        this.numAssignedWorkers.getOrElseUpdate(searchSpace, 0)
      }
      assignSearchSpaces()

    case WorkerStopped(searchSpace) =>
      numIdleWorkers += 1
      val newAssignedWorkers = numAssignedWorkers(searchSpace) - 1
      numAssignedWorkers(searchSpace) = newAssignedWorkers
      // TODO: Check whether we actually fully processed the search space.
      if (newAssignedWorkers == 0) {
        numAssignedWorkers -= searchSpace
        controller ! SearchSpaceReport(searchSpace, SearchSpaceComplete)
      }

      // Check if there are unprocessed search spaces right now.
      val numOpenSearchSpaces = numAssignedWorkers.values.count(_ == 0)
      if (numOpenSearchSpaces > 0) {
        // TODO: Consider behaving differently when just having dropped out of a search space.
        assignSearchSpaces()
      } else {
        // Otherwise, propagate new search space from controller.
        controller ! NodeManagerState(numWorkers = numWorkers, numSearchSpaces = numAssignedWorkers.size)
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
        uccConsumer(ucc)
        numDiscoveredDependencies.incrementAndGet()
      },
      (fd: PartialFD) => {
        fdConsumer(fd)
        numDiscoveredDependencies.incrementAndGet()
      }
    )
  }

  /**
    * Creates the [[Worker]] actor pool and notifies the [[controller]] of the new state.
    */
  def createWorkers() {
    // Allocate the workers.
    numWorkers =
      if (configuration.parallelism > 0) configuration.parallelism
      else Runtime.getRuntime.availableProcessors
    workerPool = context.actorOf(SmallestMailboxPool(numWorkers).props(Worker.props(profilingContext)), "worker-pool")
    numIdleWorkers = numWorkers
    log.info(s"Started $numWorkers workers.")

    // Inform the controller of the current search space.
    sender ! NodeManagerState(numWorkers = numWorkers, numSearchSpaces = numAssignedWorkers.size)
  }

  /**
    * Assign [[SearchSpace]]s to idling [[Worker]]s.
    */
  private def assignSearchSpaces(): Unit = {
    // Naive implementation. As long as there are idling workers, we just assign them whatever search space has the
    // fewest workers operating upon it.
    implicit val searchSpaceOrdering = Ordering.by[(SearchSpace, Int), Int](_._2)(Ordering.Int.reverse)
    val searchSpaceQueue = mutable.PriorityQueue[(SearchSpace, Int)](
      this.numAssignedWorkers.filter(entry => checkAdmissionForAdditionalWorker(entry._2)).toSeq: _*
    )

    while (numIdleWorkers > 0 && searchSpaceQueue.nonEmpty) {
      val (searchSpace, numWorkingWorkers) = searchSpaceQueue.dequeue()
      searchSpace.setContext(profilingContext)
      log.debug(s"Assigning $searchSpace to a worker (processed by $numWorkingWorkers other workers)")
      workerPool ! searchSpace
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
    * @param controller     that controls the new actor
    * @param configuration  that defines what to profile and how
    * @param inputPath      defines what to profile
    * @param inputGenerator optional [[RelationalInputGenerator]] to load the data from
    * @param uccConsumer    should be called whenever a new [[PartialKey]] is discovered
    * @param fdConsumer     should be called whenever a new [[PartialFD]] is discovered
    * @return the [[Props]]
    */
  def props(controller: ActorRef,
            configuration: Configuration,
            inputPath: String,
            inputGenerator: Option[RelationalInputGenerator],
            uccConsumer: PartialKey => _,
            fdConsumer: PartialFD => _) =
    Props(new NodeManager(controller, configuration, inputPath, inputGenerator, uccConsumer, fdConsumer))

  /**
    * This message asks a [[NodeManager]] actor to initialize its [[ProfilingContext]] and [[Worker]]s using the
    * [[RelationalInputGenerator]].
    */
  case object InitializeFromInputGenerator

  /**
    * This message asks a [[NodeManager]] to report the number of dependencies its [[Worker]]s discovered.
    */
  case object ReportNumDependencies

  /**
    * This message tells that some [[Worker]] stopped processing the given [[SearchSpace]].
    *
    * @param searchSpace the [[SearchSpace]]
    */
  case class WorkerStopped(searchSpace: SearchSpace)

}
