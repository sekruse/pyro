package de.hpi.isg.pyro.akka.actors

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorLogging, Props}
import de.hpi.isg.pyro.akka.actors.Collector.SignalWhenDone
import de.hpi.isg.pyro.akka.actors.Controller.CollectorComplete
import de.hpi.isg.pyro.model.{PartialFD, PartialKey}

/**
  * This [[Actor]] collects data profiling results.
  */
class Collector(optFdConsumer: Option[PartialFD => _],
                optUccConsumer: Option[PartialKey => _])
  extends Actor with ActorLogging {

  private var consumptionCounter = 0
  private var numExpectedDependencies = -1

  private val fdConsumer = optFdConsumer.getOrElse({ _: PartialFD => })
  private val uccConsumer = optUccConsumer.getOrElse({ _: PartialKey => })

  override def receive = {
    case fd: PartialFD =>
      if (log.isDebugEnabled) log.debug(s"Received $fd from ${sender()}...")
      fdConsumer(fd)
      consumptionCounter += 1
      signalWhenDone()

    case ucc: PartialKey =>
      if (log.isDebugEnabled) log.debug(s"Received $ucc from ${sender()}...")
      uccConsumer(ucc)
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
    Props(new Collector(optFdConsumer, optUccConsumer))

  /**
    * This message signals to shut down a [[Collector]] actor as soon as it has received the specified amount of
    * dependencies.
    *
    * @param numExpectedDependencies the expected number of dependencies
    */
  case class SignalWhenDone(numExpectedDependencies: Int)

}
