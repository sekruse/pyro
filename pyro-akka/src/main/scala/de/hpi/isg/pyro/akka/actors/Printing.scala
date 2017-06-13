package de.hpi.isg.pyro.akka.actors

import akka.actor.Actor

/**
  * Trait to do easy printing.
  */
trait Printing extends Actor {

  /**
    * Print the given message prefixed with an identifier of this [[Actor]].
    *
    * @param msg the message
    */
  def actorPrint(msg: String) = println(s"[${self.path}] $msg")

}
