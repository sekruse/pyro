package de.hpi.isg.pyro.akka.algorithms

import java.io.File
import java.util

import akka.actor.ActorSystem
import com.beust.jcommander.{JCommander, Parameter, Parameters, ParametersDelegate}
import de.hpi.isg.profiledb.ProfileDB
import de.hpi.isg.profiledb.store.model.{Experiment, Subject, TimeMeasurement}
import de.hpi.isg.pyro.akka.actors.{Controller, Reaper}
import de.hpi.isg.pyro.akka.utils.{AkkaUtils, Host}
import de.hpi.isg.pyro.core.Configuration
import de.hpi.isg.pyro.model.{PartialFD, PartialKey}
import de.hpi.isg.pyro.properties.MetanomePropertyLedger
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput
import de.metanome.algorithm_integration.input.RelationalInputGenerator
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
  * Main object to run Pyro.
  */
object Pyro {

  private lazy val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    // Parse the configuration.
    val profileCommand = new ProfileCommand
    val startWorkerCommand = new StartWorkerCommand
    val jCommander = new JCommander
    jCommander.addCommand("profile", profileCommand)
    jCommander.addCommand("worker", startWorkerCommand)
    try {
      jCommander.parse(args: _*)
    } catch {
      case e: Throwable =>
        println(s"Could not parse parameters. ${e.getMessage}")
        jCommander.usage()
        sys.exit(1)
    }

    jCommander.getParsedCommand match {
      case "profile" =>
        // Start Pyro.
        val experiment = profileCommand.profileDBParameters.createExperiment(profileCommand)
        try {
          profile(
            createInputMethod(profileCommand),
            OutputMethod(Some(println _), Some(println _)),
            profileCommand,
            profileCommand.master,
            profileCommand.workers,
            experiment
          )
        } catch {
          case t: Throwable =>
            println(s"Profiling failed: ${t.getMessage}")
            log.error("Profiling failed.", t)
            sys.exit(2)
        }
        experiment match {
          case Some(exp) => profileCommand.profileDBParameters.store(exp)
          case None =>
        }

      case "worker" =>
        Pyro.startWorker(startWorkerCommand.host)

      case null =>
        println(s"No command given. Available commands: profile, worker.")
        sys.exit(1)

      case other =>
        println(s"Unknown command: $other. Available commands: profile, worker.")
        sys.exit(1)
    }

  }

  /**
    * Creates a [[InputMethod]] according to the specification in the [[ProfileCommand]].
    *
    * @param profileCommand the [[ProfileCommand]]
    * @return the [[InputMethod]]
    */
  private def createInputMethod(profileCommand: ProfileCommand): InputMethod = {
    val inputPath = profileCommand.inputPath.head
    if (inputPath.startsWith("hdfs://")) HdfsInputMethod(inputPath, profileCommand.csvSettings)
    else LocalFileInputMethod(inputPath, profileCommand.csvSettings)
  }

  /**
    * Run Pyro using Akka.
    *
    * @param input         describes how to obtain the input relation to profile
    * @param output        descirbes what to do with discovered dependencies
    * @param configuration describes how to profile
    * @param master     local host to bind to when profiling with remote workers
    * @param workers       hosts to do the profiling on;
    *                      be placed on this machine;
    *                      leave blank to use a non-distributed setup
    * @throws java.lang.Exception if the profiling failed
    */
  @throws(classOf[Exception])
  def profile(input: InputMethod,
            output: OutputMethod,
            configuration: Configuration,
            master: Option[Host] = None,
            workers: Array[Host] = Array(),
            experiment: Option[Experiment] = None): Unit = {

    log.info("Start profiling with Pyro on Akka...")
    val startMillis = System.currentTimeMillis

    // Create the actor sytem.
    val (config, masterHost) = master match {
      case Some(host) =>
        (AkkaUtils.getRemoteAkkaConfig(host), host)
      case None if workers.isEmpty =>
        (AkkaUtils.getLocalAkkaConfig, Host.localhost())
      case _ =>
        (AkkaUtils.getRemoteAkkaConfig(Host.localhost()), Host.localhost())
    }
    val system = ActorSystem("pyro", config)
    Reaper(system)

    // Create a success flag to inject into the actor system to find out whether it terminated properly.
    object SuccessFlag {
      private[Pyro] var isSuccess = false
    }

    // Start a controller for the profiling.
    Controller.start(system,
      configuration,
      input,
      output,
      masterHost,
      workers,
      onSuccess = () => SuccessFlag.isSuccess = true,
      experiment
    )

    // Wait for Akka to finish its job.
    Await.ready(system.whenTerminated, 365 days)
    val elapsedMillis = System.currentTimeMillis - startMillis
    experiment foreach { exp =>
      val timeMeasurement = new TimeMeasurement("elapsedMillis")
      timeMeasurement.setMillis(elapsedMillis)
      exp.addMeasurement(timeMeasurement)
    }
    log.info(f"Profiled with Pyro in $elapsedMillis%,d ms.")
    if (!SuccessFlag.isSuccess) throw new Exception("Success flag is not set.")
  }

  /**
    * Start a new [[ActorSystem]] at the given [[Host]]. This method blocks until the [[ActorSystem]] is terminated.
    *
    * @param host the [[Host]]
    */
  def startWorker(host: Host): Unit = {
    // Create the actor sytem.
    log.info(s"Starting a new actor system on $host...")
    val system = ActorSystem("pyro", AkkaUtils.getRemoteAkkaConfig(host))
    log.info(s"Started $system.")

    // Wait for Akka to finish its job.
    Await.ready(system.whenTerminated, 365 days)
    log.info("Actor system terminated.")
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
    * Describes an [[InputMethod]] where the input relation is read from some CSV file in HDFS. We assume that HDFS
    * to be accessible from every machine the profiling runs on.
    *
    * @param url         the HDFS URL to the input CSV file(s)
    * @param csvSettings describes how to parse the CSV files
    */
  case class HdfsInputMethod(url: String, csvSettings: ConfigurationSettingFileInput) extends InputMethod

  /**
    * Describes a way of collecting the discovered dependencies.
    *
    * @param fdConsumer  optional callback for any discovered FD
    * @param uccConsumer optional callback for any discovered UCC
    */
  case class OutputMethod(fdConsumer: Option[PartialFD => _],
                          uccConsumer: Option[PartialKey => _])


  /**
    * Command-line parameters for Pyro.
    */
  @Parameters(commandDescription = "profile a dataset")
  class ProfileCommand extends de.hpi.isg.pyro.core.Configuration {

    @Parameter(names = Array("--master"), description = "<hostname>:<port> to bind to (should be local)")
    var masterDefinition: String = _

    /**
      * If Pyro should be run in the distributed mode, then this property should include a semicolon separated list of
      * hosts to run on, e.g, `"worker1:35711;worker2:35711;worker3:35711"`. The workers must be already running. A worker
      * can share an actor system with the [[masterDefinition]].
      */
    @Parameter(names = Array("--workers"), description = "list of hosts to run Pyro on, e.g. worker1:123 worker2:123" +
      "one worker may be collocated with the master; leave blank for a non-distributed set-up", variableArity = true)
    var workersDefinition: java.util.List[String] = _

    def master: Option[Host] = Option(masterDefinition).map(Host.parse)

    /**
      * Parses the [[workersDefinition]].
      *
      * @return an [[Array]] of [[Host]]s
      */
    def workers: Array[Host] = if (workersDefinition == null || workersDefinition.isEmpty) Array()
    else workersDefinition.map(Host.parse).toArray

    @Parameter(description = "path or HDFS URL to input file", required = true, arity = 1)
    var inputPath: java.util.List[String] = _

    @Parameter(names = Array("--csv-separator"), description = "CSV separator (char, semicolon, comma, pipe, tab)")
    var csvSeparator = ","

    @Parameter(names = Array("--csv-quote"), description = "CSV quote (char, single, double, none)")
    var csvQuote = "double"

    @Parameter(names = Array("--csv-escape"), description = "CSV escape charactor (char, backslash, none)")
    var csvEscapeChar = "none"

    @Parameter(names = Array("--csv-strict-quotes"), description = "strict CSV quotes")
    var csvStrictQuotes = false

    @Parameter(names = Array("--csv-header"), description = "whether there is a header line in the CSV file")
    var csvHeader = false

    @Parameter(names = Array("--csv-skip-differing-lines"), description = "whether to skip seemingly illegal CSV lines")
    var csvSkipDifferingLines = false

    @Parameter(names = Array("--csv-null-value"), description = "NULL representation")
    var csvNullValue = ""

    /**
      * Creates [[ConfigurationSettingFileInput]] as defined in this input.
      *
      * @return the [[ConfigurationSettingFileInput]]
      */
    def csvSettings = new ConfigurationSettingFileInput(
      inputPath(0),
      true,
      csvSeparator match {
        case "semicolon" => ';'
        case "comma" => ','
        case "pipe" => '|'
        case "tab" => '\t'
        case str if str.length == 1 => str.charAt(0)
        case other => sys.error(s"Unknown CSV separator ($other).")
      },
      csvQuote match {
        case "single" => '''
        case "double" => '"'
        case "none" => '\0'
        case str if str.length == 1 => str.charAt(0)
        case other => sys.error(s"Unknown CSV quote ($other).")
      },
      csvEscapeChar match {
        case "backslash" => '\\'
        case "none" => '\0'
        case str if str.length == 1 => str.charAt(0)
        case other => sys.error(s"Unknown CSV escape character ($other).")
      },
      csvStrictQuotes,
      true,
      0,
      csvHeader,
      csvSkipDifferingLines,
      csvNullValue
    )

    @ParametersDelegate
    val profileDBParameters = new ProfileDBParameters

    // TODO: Metacrate parameters...
  }

  /**
    * Parameters specific to the initialization of a ProfileDB [[Experiment]].
    */
  class ProfileDBParameters {

    @Parameter(names = Array("--pdb"), description = "store ProfileDB experiments at this location")
    var location: String = _

    @Parameter(names = Array("--pdb-id"), description = "ID for the ProfileDB experiment")
    var id: String = _

    @Parameter(names = Array("--pdb-tags"), description = "tags for ProfileDB experiments", variableArity = true)
    var tags: java.util.List[String] = new java.util.LinkedList

    @Parameter(names = Array("--pdb-conf"), description = "additional configuration values (<key>:<value>...)", variableArity = true)
    var confSpec: java.util.List[String] = new java.util.LinkedList

    def isSpecified: Boolean = location != null && location.nonEmpty && id != null && id.nonEmpty

    def createExperiment(profileCommand: ProfileCommand): Option[Experiment] = {
      if (!isSpecified) return None

      val experiment = new Experiment(id, new Subject("Pyro (Akka)", "1.0"), tags: _*)
      val subject = experiment.getSubject
      subject.addConfiguration("input", profileCommand.inputPath)
      subject.addConfiguration("workers", profileCommand.workers)
      val propertyLedger = MetanomePropertyLedger.createFor(profileCommand)
      propertyLedger.getProperties foreach {
        case (key, manager) =>
          var value = manager.get(profileCommand)
          value match {
            case double: java.lang.Double if double.isInfinite || double.isNaN =>
            case _ => subject.addConfiguration(key, value)
          }
      }

      val confSpecPattern = """([a-zA-Z_][^:]*):(.*)""".r
      confSpec.foreach {
        case confSpecPattern(key, value) => subject.addConfiguration(key, value)
        case other => throw new IllegalArgumentException(s"Cannot parse ProfileDB configuration: '$other'.")
      }

      Some(experiment)
    }

    def store(experiment: Experiment): Unit = {
      assert(isSpecified)

      Try {
        val profileDB = new ProfileDB
        profileDB.append(new File(location), experiment)
      } match {
        case Success(_) => log.info(s"Stored experiment ${experiment.getId} to $location.")
        case Failure(throwable) => log.error("Failed to store experiment.", throwable)
      }
    }

  }

  /**
    * Configures for starting a passive worker.
    */
  @Parameters(commandDescription = "start a passive worker")
  class StartWorkerCommand {

    @Parameter(description = "the host and port to bind the worker to, e.g., worker2:123", arity = 1)
    var hostDefinition: java.util.List[String] = new util.ArrayList(1)

    /**
      * Provides the host to bind a worker to.
      *
      * @return the [[Host]]
      */
    def host: Host =
      if (hostDefinition.isEmpty) Host.localhost()
      else Host.parse(hostDefinition(0))

  }

}
