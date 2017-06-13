package de.hpi.isg.pyro.akka.actors

import akka.actor.Actor

/**
  * This [[Actor]] performs actual profiling of a dataset.
  */
class Worker extends Actor with Printing {

  override def receive = {
    case msg => actorPrint(s"I am a dummy worker and cannot handle $msg.")
  }

}
