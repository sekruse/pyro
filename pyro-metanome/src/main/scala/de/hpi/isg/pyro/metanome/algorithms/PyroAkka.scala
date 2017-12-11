package de.hpi.isg.pyro.metanome.algorithms

import java.lang.Boolean
import java.util

import de.hpi.isg.mdms.clients.MetacrateClient
import de.hpi.isg.mdms.model.MetadataStore
import de.hpi.isg.pyro.akka.algorithms.Pyro
import de.hpi.isg.pyro.akka.algorithms.Pyro.{OutputMethod, RelationalInputGeneratorInputMethod}
import de.hpi.isg.pyro.akka.utils.Host
import de.hpi.isg.pyro.core.{Configuration => PyroConfiguration}
import de.hpi.isg.pyro.metanome.algorithms
import de.hpi.isg.pyro.model.{PartialFD, PartialKey}
import de.hpi.isg.pyro.properties.{MetanomeProperty, MetanomePropertyLedger}
import de.metanome.algorithm_integration.algorithm_types._
import de.metanome.algorithm_integration.configuration.{ConfigurationRequirement, ConfigurationRequirementFileInput, ConfigurationSetting}
import de.metanome.algorithm_integration.input.RelationalInputGenerator
import de.metanome.algorithm_integration.result_receiver.{FunctionalDependencyResultReceiver, UniqueColumnCombinationResultReceiver}
import de.metanome.algorithm_integration.{AlgorithmConfigurationException, AlgorithmExecutionException}


class PyroAkka extends MetacrateClient
  with FunctionalDependencyAlgorithm
  with UniqueColumnCombinationsAlgorithm
  with IntegerParameterAlgorithm
  with StringParameterAlgorithm
  with RelationalInputParameterAlgorithm
  with BooleanParameterAlgorithm {

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
  private var configuration: PyroAkka.Configuration = new algorithms.PyroAkka.Configuration

  /**
    * Utility to serve Metanome properties from the [[configuration]] via reflection.
    */
  private lazy val propertyLedger: MetanomePropertyLedger = MetanomePropertyLedger.createFor(configuration)

  override def getConfigurationRequirements: util.ArrayList[ConfigurationRequirement[_ <: ConfigurationSetting]] = {
    val configurationRequirement = new util.ArrayList[ConfigurationRequirement[_ <: ConfigurationSetting]]
    try
      propertyLedger.contributeConfigurationRequirements(configurationRequirement)
    catch {
      case e: AlgorithmConfigurationException => throw new RuntimeException(e)
    }
    {
      val requirement = new ConfigurationRequirementFileInput("inputFile")
      requirement.setRequired(true)
      configurationRequirement.add(requirement)
    }
    configurationRequirement
  }

  override def setMetadataStore(metadataStore: MetadataStore): Unit = this.metadataStore = metadataStore

  override def setResultReceiver(resultReceiver: FunctionalDependencyResultReceiver): Unit = fdResultReceiver = resultReceiver

  override def setResultReceiver(resultReceiver: UniqueColumnCombinationResultReceiver): Unit = uccResultReceiver = resultReceiver

  override def setIntegerConfigurationValue(identifier: String, values: Integer*): Unit =
    propertyLedger.configure(configuration, identifier, values: _*)

  override def setStringConfigurationValue(identifier: String, values: String*): Unit =
    propertyLedger.configure(configuration, identifier, values: _*)

  override def setBooleanConfigurationValue(identifier: String, values: Boolean*): Unit =
    propertyLedger.configure(configuration, identifier, values: _*)

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

  @throws(classOf[AlgorithmExecutionException])
  override def execute(): Unit = {
    if (configuration.isInitialPause) {
      scala.io.StdIn.readLine("Press Enter to continue...")
    }
    try {
      Pyro.profile(
        input = RelationalInputGeneratorInputMethod(this.inputGenerator),
        output = OutputMethod(
          fdConsumer = Some((fd: PartialFD) => this.fdResultReceiver.receiveResult(fd.toMetanomeFunctionalDependency)),
          uccConsumer = Some((ucc: PartialKey) => this.uccResultReceiver.receiveResult(ucc.toMetanomeUniqueColumnCobination))
        ),
        master = configuration.masterHostOption,
        workers = configuration.workerHosts,
        configuration = configuration
      )
    } catch {
      case e: Throwable => throw new AlgorithmExecutionException("Pyro failed.", e)
    }
  }
}

object PyroAkka {

  class Configuration extends PyroConfiguration {

    /**
      * Optional `host:port` specification to bind the master to.
      */
    @MetanomeProperty
    var master: String = null

    /**
      * Optional comma-separated `host:port` specifications of workers to profile with.
      */
    @MetanomeProperty
    var workers: String = ""

    def masterHostOption: Option[Host] = Option(master).map(Host.parse)

    def workerHosts: Array[Host] = workers.split(",").map(Host.parse)

  }

}

