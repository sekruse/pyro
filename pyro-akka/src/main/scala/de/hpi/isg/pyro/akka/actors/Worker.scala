package de.hpi.isg.pyro.akka.actors

import akka.actor.Actor
import de.hpi.isg.pyro.core.ProfilingContext

/**
  * This [[Actor]] performs actual profiling of a dataset.
  */
class Worker extends Actor with Printing {

  private var profilingContext: ProfilingContext = _

  override def receive = {
    case profilingContext: ProfilingContext =>
      this.profilingContext = profilingContext

    case msg => actorPrint(s"I am a dummy worker and cannot handle $msg.")
  }

}
