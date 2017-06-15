package de.hpi.isg.pyro.akka.utils

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Utilities to work with Akka.
  */
object AkkaUtils {

  /**
    * The system name used by Pyro for [[ActorSystem]]s.
    */
  val systemName = "pyro"

  /**
    * Creates a Akka [[Config]] for a local setup.
    *
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
    *
    * @param host the host that the [[ActorSystem]] should bind to
    * @return the [[ActorSystem]]
    */
  def getRemoteAkkaConfig(host: Host): Config = getRemoteAkkaConfig(host.hostName, host.port)

  /**
    * Creates a remote-capable, TCP-based [[ActorSystem]].
    *
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
