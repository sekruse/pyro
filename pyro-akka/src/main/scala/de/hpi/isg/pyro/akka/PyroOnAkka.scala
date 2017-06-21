package de.hpi.isg.pyro.akka

import akka.actor.ActorSystem
import akka.serialization.{SerializationExtension, SerializationSetup, Serializer, SerializerDetails}
import de.hpi.isg.pyro.akka.actors.Controller
import de.hpi.isg.pyro.akka.utils.{AkkaUtils, Host, MySerializationExtension}
import de.hpi.isg.pyro.core.Configuration
import de.hpi.isg.pyro.model.{PartialFD, PartialKey}
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput
import de.metanome.algorithm_integration.input.RelationalInputGenerator
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * This is the main entry point to run Pyro on Akka.
  */
object PyroOnAkka {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  /**
    * Run Pyro using Akka.
    *
    * @param input         describes how to obtain the input relation to profile
    * @param output        descirbes what to do with discovered dependencies
    * @param configuration describes how to profile
    * @param hosts         hosts to do the profiling on;
    *                      the first host determines where to run the Akka's controller and result receiver and should
    *                      be placed on this machine;
    *                      leave blank to use a non-distributed setup
    * @throws java.lang.Exception if the profiling failed
    */
  @throws(classOf[Exception])
  def apply(input: InputMethod,
            output: OutputMethod,
            configuration: Configuration,
            hosts: Array[Host] = Array()): Unit = {

    logger.info("Start profiling with Pyro on Akka...")
    val startMillis = System.currentTimeMillis

    // Create the actor sytem.
    val system = ActorSystem("pyro",
      if (hosts.isEmpty) AkkaUtils.getLocalAkkaConfig
      else AkkaUtils.getRemoteAkkaConfig(hosts.head)
    )

    // Create a success flag to inject into the actor system to find out whether it terminated properly.
    object SuccessFlag {
      private[PyroOnAkka] var isSuccess = false
    }

    // Start a controller for the profiling.
    Controller.start(system, configuration,
      input,
      output,
      hosts,
      onSuccess = () => SuccessFlag.isSuccess = true
    )

    // Wait for Akka to finish its job.
    Await.ready(system.whenTerminated, 365 days)
    logger.info(f"Profiled with Pyro in ${System.currentTimeMillis - startMillis}%,d ms.")
    if (!SuccessFlag.isSuccess) throw new Exception("Success flag is not set.")
  }

  /**
    * Start a new [[ActorSystem]] at the given [[Host]]. This method blocks until the [[ActorSystem]] is terminated.
    * @param host the [[Host]]
    */
  def startWorker(host: Host): Unit = {
    // Create the actor sytem.
    logger.info("Starting a new actor system...")
    val system = ActorSystem("pyro", AkkaUtils.getRemoteAkkaConfig(host))
    logger.info(s"Started $system.")

    // Wait for Akka to finish its job.
    Await.ready(system.whenTerminated, 365 days)
    logger.info("Actor system terminated.")
  }

  /**
    * Describes a way of obtaining the input data to profile.
    */
  sealed trait InputMethod

  /**
    * Describes an [[InputMethod]] that uses a [[RelationalInputGenerator]] provided by Metanome.
    *
    * @param generator the [[RelationalInputGenerator]]
    */
  case class RelationalInputGeneratorInputMethod(generator: RelationalInputGenerator) extends InputMethod

  /**
    * Describes an [[InputMethod]] where the input relation is read from some local CSV file. We assume that CSV file
    * to be present on every machine the profiling runs on.
    *
    * @param inputPath   the (local filesystem) path to the input CSV file(s)
    * @param csvSettings describes how to parse the CSV files
    */
  case class LocalFileInputMethod(inputPath: String, csvSettings: ConfigurationSettingFileInput) extends InputMethod

  /**
    * Describes a way of collecting the discovered dependencies.
    *
    * @param fdConsumer  optional callback for any discovered FD
    * @param uccConsumer optional callback for any discovered UCC
    */
  case class OutputMethod(fdConsumer: Option[PartialFD => _],
                          uccConsumer: Option[PartialKey => _])


}
