package de.hpi.isg.pyro.akka.actors

import akka.actor.{Actor, ActorRef, ActorSystem, Address, Deploy, Props}
import akka.remote.RemoteScope
import de.hpi.isg.pyro.akka.messages.{NodeManagerReady, StartProfiling, WorkersReady}

import scala.collection.mutable

/**
  * The purpose of this [[Actor]] is to steer the basic execution of Pyro.
  */
class Controller extends Actor with Printing {

  /**
    * References to the controlled [[NodeManager]]s.
    */
  var nodeManagers: mutable.Map[ActorRef, Int] = mutable.Map()

  /**
    * The file that is currently being profiled.
    */
  var path: String = _


  override def receive = {
    case StartProfiling(_path, hosts) =>
      this.path = path

      // Create node managers.
      if (hosts.isEmpty) {
        // Create a local node manager only.
        actorPrint("Create local node manager.")
        val localNodeManager = context.actorOf(Props[NodeManager])
        nodeManagers(localNodeManager) = 0
      } else {
        // Create remote node managers.
        hosts.foreach { case (host, port) =>
          actorPrint(s"Create remote node manager at $host:$port.")
          val deploy = new Deploy(RemoteScope(new Address("akka.tcp", "pyro", host, port)))
          val remoteNodeManager = context.actorOf(Props[NodeManager].withDeploy(deploy))
          nodeManagers(remoteNodeManager) = 0
        }
      }

      // Start the node managers.
      nodeManagers.foreach(_._1 ! StartProfiling(_path, Array()))

      // Change mode.
      context.become(started)

    case NodeManagerReady() =>
      nodeManagers.getOrElseUpdate(sender(), 0)

    case WorkersReady(n) =>
      nodeManagers(sender()) = n

    case other =>
      sys.error(s"[${self.path}] Cannot handle $other")
  }

  def started: Actor.Receive = {
    case NodeManagerReady() =>
      sender() ! StartProfiling(path, Array())
      nodeManagers.getOrElseUpdate(sender(), 0)

    case WorkersReady(n) =>
      nodeManagers(sender()) = n
      actorPrint(s"Should send $n tasks to ${sender().path}")

    case other => sys.error(s"[${self.path}] Cannot handle $other")
  }

}

/**
  * Utilities to work with [[Controller]]s.
  */
object Controller {

  /**
    * Sets up a [[Controller]] in the [[ActorSystem]] and starts it.
    *
    * @param actorSystem the [[ActorSystem]]
    * @param path        the path of the dataset to be profiled
    */
  def start(actorSystem: ActorSystem, path: String, hosts: Array[(String, Int)]) = {
    val controller = actorSystem.actorOf(Props[Controller], "controller")
    controller ! StartProfiling(path, hosts)
  }

}