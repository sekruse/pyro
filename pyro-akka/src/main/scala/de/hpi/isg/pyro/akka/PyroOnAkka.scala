package de.hpi.isg.pyro.akka

import akka.actor.ActorSystem
import de.hpi.isg.pyro.akka.actors.{Controller, NodeManager}
import de.hpi.isg.pyro.akka.utils.AkkaUtils

/**
  * TODO
  */
object PyroOnAkka {

  def main(args: Array[String]): Unit = {
    // Parse the hosts to run on.
    val hosts = args.map { arg =>
      val Array(host, port) = arg.split(":")
      (host, port.toInt)
    }

    // Create the actor sytem.
    val system = ActorSystem("pyro",
      if (hosts.isEmpty) AkkaUtils.getLocalAkkaConfig
      else AkkaUtils.getRemoteAkkaConfig(hosts(0)._1, hosts(0)._2)
    )

    // Set up a local node manager only.
    NodeManager.createOn(system)

    // Start a controller for the profiling.
    Controller.start(system, "/my/file/to/profile", hosts)

    Thread.sleep(5000)
    system.terminate()
  }

}
