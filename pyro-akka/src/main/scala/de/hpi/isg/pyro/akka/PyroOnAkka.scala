package de.hpi.isg.pyro.akka

import java.lang.Boolean
import java.util

import akka.actor.ActorSystem
import de.hpi.isg.mdms.clients.MetacrateClient
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.pyro.akka.actors.Controller
import de.hpi.isg.pyro.akka.utils.{AkkaUtils, Host}
import de.hpi.isg.pyro.core.Configuration
import de.hpi.isg.pyro.properties.MetanomePropertyLedger
import de.metanome.algorithm_integration.{AlgorithmConfigurationException, AlgorithmExecutionException}
import de.metanome.algorithm_integration.algorithm_types._
import de.metanome.algorithm_integration.configuration.{ConfigurationRequirement, ConfigurationRequirementFileInput, ConfigurationSetting}
import de.metanome.algorithm_integration.input.RelationalInputGenerator
import de.metanome.algorithm_integration.result_receiver.{FunctionalDependencyResultReceiver, UniqueColumnCombinationResultReceiver}
import de.metanome.backend.result_receiver.ResultReceiver
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration._


class PyroOnAkka extends MetacrateClient
  with FunctionalDependencyAlgorithm
  with UniqueColumnCombinationsAlgorithm
  with IntegerParameterAlgorithm
  with StringParameterAlgorithm
  with RelationalInputParameterAlgorithm
  with BooleanParameterAlgorithm {

  private val logger = LoggerFactory.getLogger(getClass)


  /**
    * If a [[MetadataStore]] is set, then it should be used to collect dependencies.
    */
  private var metadataStore: MetadataStore = _

  /**
    * Metanome's way of providing input data.
    */
  private var inputGenerator: RelationalInputGenerator = _

  /**
    * Metanome's way of collecting dependencies.
    */
  private var fdResultReceiver: FunctionalDependencyResultReceiver = _

  /**
    * Metanome's way of collecting dependencies.
    */
  private var uccResultReceiver: UniqueColumnCombinationResultReceiver = _

  /**
    * Maintains the configuration of Pyro.
    */
  private var configuration: Configuration = new Configuration

  /**
    * Utility to serve Metanome properties from the [[configuration]] via reflection.
    */
  private lazy val propertyLedger: MetanomePropertyLedger = MetanomePropertyLedger.createFor(configuration)

  override def getConfigurationRequirements: util.ArrayList[ConfigurationRequirement[_ <: ConfigurationSetting]] = {
    val configurationRequirement = new util.ArrayList[ConfigurationRequirement[_ <: ConfigurationSetting]]
    try
      propertyLedger.contributeConfigurationRequirements(configurationRequirement)
    catch {
      case e: AlgorithmConfigurationException => {
        throw new RuntimeException(e)
      }
    }
    {
      val requirement = new ConfigurationRequirementFileInput(PyroOnAkka.inputGeneratorConfigKey)
      requirement.setRequired(true)
      configurationRequirement.add(requirement)
    }
    configurationRequirement
  }

  override def setMetadataStore(metadataStore: MetadataStore): Unit = this.metadataStore = metadataStore

  override def setResultReceiver(resultReceiver: FunctionalDependencyResultReceiver): Unit = fdResultReceiver = resultReceiver

  override def setResultReceiver(resultReceiver: UniqueColumnCombinationResultReceiver): Unit = uccResultReceiver = resultReceiver

  override def setIntegerConfigurationValue(identifier: String, values: Integer*): Unit =
    propertyLedger.configure(configuration, identifier, values)

  override def setStringConfigurationValue(identifier: String, values: String*): Unit =
    propertyLedger.configure(configuration, identifier, values)

  override def setBooleanConfigurationValue(identifier: String, values: Boolean*): Unit =
    propertyLedger.configure(configuration, identifier, values)

  override def setRelationalInputConfigurationValue(identifier: String, values: RelationalInputGenerator*): Unit = {
    require(values.size == 1, s"Exactly one relational input required, but found ${values.size}.")
    inputGenerator = values.head
  }

  override def getAuthors: String = "Sebastian Kruse"

  override def getDescription: String =
    """Pyro uses a depth-first traversal strategy to find approximate UCCs and FDs.
      |This implementation uses Akka to distribute the different search paths among cores and/or among machines in a
      |cluster.
    """.stripMargin

  override def execute(): Unit = {
    logger.info("Start profiling with Pyro on Akka...")
    val startMillis = System.currentTimeMillis

    val hosts =
      if (configuration.hosts == null || configuration.hosts.isEmpty) Array[Host]()
      else configuration.hosts.split(";").map(Host.parse)

    // Create the actor sytem.
    val system = ActorSystem("pyro",
      if (hosts.isEmpty) AkkaUtils.getLocalAkkaConfig
      else AkkaUtils.getRemoteAkkaConfig(hosts.head)
    )

    object SuccessFlag { private[PyroOnAkka] var isSuccess = false }

    // Start a controller for the profiling.
    Controller.start(system, configuration,
      inputPath = "(unknown path)",
      inputGenerator = Some(inputGenerator),
      uccConsumer = Some(println),
      fdConsumer = Some(println),
      hosts = hosts,
      onSuccess = () => SuccessFlag.isSuccess = true
    )

    // Wait for Akka to finish its job.
    Await.ready(system.whenTerminated, 365 days)
    logger.info(f"Profiled with Pyro in ${System.currentTimeMillis - startMillis}%,d ms.")
    if (!SuccessFlag.isSuccess) throw new AlgorithmExecutionException("Success flag is not set.")
  }
}

/**
  * This is the main entry point to run Pyro on Akka.
  */
object PyroOnAkka {

  private val inputGeneratorConfigKey = "inputFile"

  def apply(configuration: Configuration,
            inputPath: String,
            inputGenerator: Option[RelationalInputGenerator] = None,
            resultReceiver: Option[ResultReceiver] = None,
            hosts: Array[Host] = Array()): Unit = {


    val pyro = new PyroOnAkka
    pyro.configuration = configuration
    inputGenerator foreach { e => pyro.setRelationalInputConfigurationValue(inputGeneratorConfigKey, e) }
    resultReceiver foreach { e =>
      if (e.isInstanceOf[UniqueColumnCombinationResultReceiver]) pyro.setResultReceiver(e.asInstanceOf[UniqueColumnCombinationResultReceiver])
      if (e.isInstanceOf[FunctionalDependencyResultReceiver]) pyro.setResultReceiver(e.asInstanceOf[FunctionalDependencyResultReceiver])
    }

    pyro.execute()
  }

}
