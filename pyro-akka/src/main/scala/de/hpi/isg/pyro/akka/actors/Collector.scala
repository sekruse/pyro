package de.hpi.isg.pyro.akka.actors

import akka.actor.{Actor, ActorLogging, Props, SupervisorStrategy}
import de.hpi.isg.pyro.akka.actors.Collector.{DiscoveredFD, DiscoveredUCC, InitializeCollector, SignalWhenDone}
import de.hpi.isg.pyro.akka.actors.Controller.CollectorComplete
import de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg
import de.hpi.isg.pyro.akka.utils.AkkaUtils
import de.hpi.isg.pyro.model.{PartialFD, PartialKey, RelationSchema}

/**
  * This [[Actor]] collects data profiling results.
  */
class Collector(optFdConsumer: Option[PartialFD => _],
                optUccConsumer: Option[PartialKey => _])
  extends Actor with ActorLogging {

  private var consumptionCounter = 0
  private var numExpectedDependencies = -1

  implicit private var schema: RelationSchema = _

  private val fdConsumer = optFdConsumer.getOrElse({ _: PartialFD => })
  private val uccConsumer = optUccConsumer.getOrElse({ _: PartialKey => })

  override val supervisorStrategy: SupervisorStrategy = AkkaUtils.escalateSupervisorStrategy

  override def receive = {
    case InitializeCollector(relationSchema) =>
      schema match {
        case null => schema = relationSchema
      }

    case DiscoveredFD(partialFD) =>
      if (log.isDebugEnabled) log.debug(s"Received $partialFD from ${sender()}...")
      fdConsumer(partialFD)
      consumptionCounter += 1
      signalWhenDone()

    case DiscoveredUCC(partialKey) =>
      if (log.isDebugEnabled) log.debug(s"Received $partialKey from ${sender()}...")
      uccConsumer(partialKey)
      consumptionCounter += 1
      signalWhenDone()

    case SignalWhenDone(numExpectedDeps) =>
      numExpectedDependencies = numExpectedDeps
      signalWhenDone()

    case msg => log.error(s"I am a dummy worker and cannot handle $msg.")
  }

  /**
    * Send a [[de.hpi.isg.pyro.akka.actors.Controller.CollectorComplete]] message to the parent if the expected
    * number of dependencies has been reached.
    */
  @inline
  private def signalWhenDone(): Unit = {
    if (numExpectedDependencies > -1 && numExpectedDependencies >= consumptionCounter) {
      context.parent ! CollectorComplete
    }
  }

}

/**
  * Companion object.
  */
object Collector {

  /**
    * Creates a [[Props]] instance for a new [[Collector]].
    *
    * @param optFdConsumer  an optional function that consumes any received [[PartialFD]]
    * @param optUccConsumer an optional function that consumes any received [[PartialKey]]
    * @return the [[Props]] instance
    */
  def props(optFdConsumer: Option[PartialFD => _],
            optUccConsumer: Option[PartialKey => _]) =
    Props(classOf[Collector], optFdConsumer, optUccConsumer)

  /**
    * This message asks to initialize a [[Collector]].
    *
    * @param schema to do the initialization
    */
  case class InitializeCollector(schema: RelationSchema)

  /**
    * This message signals to shut down a [[Collector]] actor as soon as it has received the specified amount of
    * dependencies.
    *
    * @param numExpectedDependencies the expected number of dependencies
    */
  case class SignalWhenDone(numExpectedDependencies: Int)

  /**
    * This message transports a discovered [[PartialFD]]. Note that this message is put as a [[DependencyMsg]]
    * on the wire.
    */
  object DiscoveredFD {

    def apply(partialFD: PartialFD): DependencyMsg = {
      val builder = DependencyMsg.newBuilder()
        .setDependencyType(DependencyMsg.DependencyType.FD)
        .setError(partialFD.error)
        .setScore(partialFD.score)
        .setRhs(partialFD.rhs.getIndex)
      for (lhs <- partialFD.lhs.getColumns) builder.addLhs(lhs.getIndex)
      builder.build()
    }

    def unapply(dependencyMsg: DependencyMsg)(implicit schema: RelationSchema): Option[PartialFD] =
      dependencyMsg.getDependencyType match {
        case DependencyMsg.DependencyType.FD =>
          Some(new PartialFD(
            schema.getVertical(dependencyMsg.getLhsList),
            schema.getColumn(dependencyMsg.getRhs),
            dependencyMsg.getError,
            dependencyMsg.getScore
          ))
        case _ => None
      }

  }

  /**
    * This message transports a discovered [[PartialKey]]. Note that this message is put as a [[DependencyMsg]]
    * on the wire.
    */
  object DiscoveredUCC {

    def apply(partialKey: PartialKey): DependencyMsg = {
      val builder = DependencyMsg.newBuilder()
        .setDependencyType(DependencyMsg.DependencyType.UCC)
        .setError(partialKey.error)
        .setScore(partialKey.score)
      for (lhs <- partialKey.vertical.getColumns) builder.addLhs(lhs.getIndex)
      builder.build()
    }

    def unapply(dependencyMsg: DependencyMsg)(implicit schema: RelationSchema): Option[PartialKey] =
      dependencyMsg.getDependencyType match {
        case DependencyMsg.DependencyType.UCC =>
          Some(new PartialKey(
            schema.getVertical(dependencyMsg.getLhsList),
            dependencyMsg.getError,
            dependencyMsg.getScore
          ))
        case _ => None
      }

  }

}
