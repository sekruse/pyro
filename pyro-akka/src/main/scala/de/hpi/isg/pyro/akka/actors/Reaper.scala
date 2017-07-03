package de.hpi.isg.pyro.akka.actors

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Terminated}
import de.hpi.isg.pyro.akka.actors.Reaper.WatchTask

/**
  * The reaper watches some [[Actor]]s and shuts down its [[akka.actor.ActorSystem]] as soon as any of them terminates.
  */
class Reaper extends Actor with ActorLogging {

  override def receive: Receive = {
    case WatchTask(actor) =>
      log.info(s"Watching $actor...")
      context.watch(actor)

    case Terminated(actor) =>
      log.info(s"Detected termination of $actor. Shutting down.")
      context.system.terminate()
  }

}

object Reaper {

  /**
    * Tell a [[Reaper]] to watch the given actor.
    *
    * @param actorRef the actor
    */
  case class WatchTask(actorRef: ActorRef)

  /**
    * [[Props]] to create a new [[Reaper]]
    *
    * @return the [[Props]]
    */
  def props = Props[Reaper]

  /**
    * Create a [[Reaper]] for the given [[ActorSystem]].
    */
  def apply(actorSystem: ActorSystem): ActorRef = actorSystem.actorOf(props, "reaper")

  /**
    * Select the [[Reaper]] of the [[ActorSystem]] if any.
    */
  def select(actorSystem: ActorSystem) = actorSystem.actorSelection(actorSystem / "reaper")


}

