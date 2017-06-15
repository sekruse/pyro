package de.hpi.isg.pyro.akka

import akka.actor.ActorSystem
import de.hpi.isg.pyro.akka.actors.{Controller, NodeManager}
import de.hpi.isg.pyro.akka.utils.{AkkaUtils, Host}
import de.hpi.isg.pyro.core.Configuration
import de.metanome.algorithm_integration.input.RelationalInputGenerator
import de.metanome.backend.result_receiver.ResultReceiver

/**
  * This is the main entry point to run Pyro on Akka.
  */
object PyroOnAkka {

  def apply(configuration: Configuration,
            inputPath: String,
            inputGenerator: Option[RelationalInputGenerator] = None,
            resultReceiver: Option[ResultReceiver] = None,
            hosts: Array[Host] = Array()): Unit = {

    // Create the actor sytem.
    val system = ActorSystem("pyro",
      if (hosts.isEmpty) AkkaUtils.getLocalAkkaConfig
      else AkkaUtils.getRemoteAkkaConfig(hosts.head)
    )

    // Set up a local node manager only.
    NodeManager.createOn(system)

    // Start a controller for the profiling.
    Controller.start(system, configuration, inputPath, inputGenerator, resultReceiver, hosts)

//    Thread.sleep(5000)
//    system.terminate()
    Thread.sleep(60 * 60 * 1000) // 1h
  }



}
