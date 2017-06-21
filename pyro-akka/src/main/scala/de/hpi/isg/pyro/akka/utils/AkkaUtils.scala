package de.hpi.isg.pyro.akka.utils

import akka.actor.AbstractActor.ActorContext
import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{ActorRef, ActorSystem, OneForOneStrategy, SupervisorStrategy}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.Future
import scala.language.postfixOps
import scala.reflect.ClassTag

/**
  * Utilities to work with Akka.
  */
object AkkaUtils {

  /**
    * The system name used by Pyro for [[ActorSystem]]s.
    */
  val systemName = "pyro"

  /**
    * The basic [[Config]] for Akka.
    */
  val basicConfig: Config = {
    val configString =
      """akka {
        |  loggers = ["akka.event.slf4j.Slf4jLogger"]
        |  loglevel = "DEBUG"
        |  logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"
        |  actor {
        |    serializers {
        |      java = "akka.serialization.JavaSerializer"
        |      proto = "akka.remote.serialization.ProtobufSerializer"
        |      kryo = "com.twitter.chill.akka.ConfiguredAkkaSerializer"
        |    }
        |    serialization-bindings {
        |      "de.hpi.isg.pyro.akka.protobuf.Messages$DependencyMsg" = proto
        |      "java.io.Serializable" = kryo
        |    }
        |    // serialize-messages = off
        |  }
        |}
        |com.twitter.chill.config.configuredinstantiator = "de.hpi.isg.pyro.akka.utils.SerializationUtils$PyroKryoInstantiator$"
      """.stripMargin
    ConfigFactory.parseString(configString)
  }

  /**
    * Creates a Akka [[Config]] for a local setup.
    *
    * @return the [[Config]]
    */
  def getLocalAkkaConfig: Config = basicConfig

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
    basicConfig withFallback ConfigFactory.parseString(configString)
  }

  /**
    * Ask all given [[ActorRef]]s some message and constructs a [[Future]] with a [[Map]] containing the responses
    * indexed by the actor.
    *
    * @param actors       the [[ActorRef]]s to be asked
    * @param msg          the message to ask
    * @param timeout      the [[Timeout]] for the interaction
    * @param actorContext the [[ActorContext]]
    * @tparam T the result type
    * @return a [[Future]] with a [[Map]] containing the responses
    */
  def askAll[T: ClassTag, V](actors: Iterable[ActorRef], msg: Any, callback: (ActorRef, T) => _)
                            (implicit timeout: Timeout, actorContext: ActorContext)
  : Unit = {
    implicit val executionContext = actorContext.dispatcher
    actors.foreach { actor =>
      (actor ? msg).mapTo[T] onSuccess {
        case reply: T => callback(actor, reply)
      }
    }
  }

  /**
    * Returns a [[SupervisorStrategy]] that immediately escalates.
    *
    * @return the [[SupervisorStrategy]]
    */
  def escalateSupervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _ => Escalate
  }

}
