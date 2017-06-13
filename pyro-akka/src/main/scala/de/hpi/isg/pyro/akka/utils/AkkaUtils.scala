package de.hpi.isg.pyro.akka.utils

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Utilities to work with Akka.
  */
object AkkaUtils {

  /**
    * Creates a Akka [[Config]] for a local setup.
    * @return the [[Config]]
    */
  def getLocalAkkaConfig: Config = {
    val configString =
      """akka {
        |}
      """.stripMargin
    ConfigFactory.parseString(configString)
  }

  /**
    * Creates a remote-capable, TCP-based [[ActorSystem]].
    * @param host the host name that the [[ActorSystem]] should bind to
    * @param port the port that the [[ActorSystem]] should bind to
    * @return the [[ActorSystem]]
    */
  def getRemoteAkkaConfig(host: String, port: Int): Config = {
    val configString =
      s"""akka {
         |  actor {
         |    provider = remote
         |  }
         |  remote {
         |    enabled-transports = ["akka.remote.netty.tcp"]
         |    netty.tcp {
         |      hostname = "$host"
         |      port = $port
         |    }
         | }
         |}
      """.stripMargin
    ConfigFactory.parseString(configString)
  }

}
