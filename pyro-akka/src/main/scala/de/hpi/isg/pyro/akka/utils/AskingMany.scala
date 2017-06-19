package de.hpi.isg.pyro.akka.utils

import akka.actor.AbstractActor.ActorContext
import akka.actor.{Actor, ActorRef}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag

/**
  * This trait helps to issue asks to many receivers.
  */
trait AskingMany extends Actor {

  /**
    * Ask all given [[ActorRef]]s some message and constructs a [[Future]] with a [[Map]] containing the responses
    * indexed by the actor. This method is
    *
    * @param actors       the [[ActorRef]]s to be asked
    * @param msg          the message to ask
    * @param timeout      the [[Timeout]] for the interaction
    * @tparam T the result type
    * @return a [[Map]] containing the responses
    */
  def askAll[T: ClassTag](actors: Iterable[ActorRef], msg: Any)
                            (implicit timeout: Timeout)
  : Map[ActorRef, T] = {
    implicit val executionContext = context.dispatcher
    val future = Future.traverse(actors)(actor => (actor ? msg).mapTo[T].map(actor -> _))
    Await.result(future, timeout.duration).toMap
  }

}
