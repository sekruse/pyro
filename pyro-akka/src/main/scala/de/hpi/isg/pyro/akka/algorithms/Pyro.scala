package de.hpi.isg.pyro.akka.algorithms

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import de.hpi.isg.pyro.akka.PyroOnAkka
import de.hpi.isg.pyro.akka.PyroOnAkka.{LocalFileInputMethod, OutputMethod}
import de.hpi.isg.pyro.akka.utils.Host
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput

import scala.collection.JavaConversions._

/**
  * Main object to run Pyro.
  */
object Pyro {

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
        try {
          PyroOnAkka(
            LocalFileInputMethod(profileCommand.inputPath(0), profileCommand.csvSettings),
            OutputMethod(Some(println _), Some(println _)),
            profileCommand,
            profileCommand.hosts
          )
        } catch {
          case e: Throwable =>
            println("Profiling failed.")
            sys.exit(2)
        }

      case "worker" =>
        PyroOnAkka.startWorker(startWorkerCommand.host)

      case null =>
        println(s"No command given. Available commands: profile, worker.")
        sys.exit(1)

      case other =>
        println(s"Unknown command: $other. Available commands: profile, worker.")
        sys.exit(1)
    }
  }

  /**
    * Command-line parameters for Pyro.
    */
  @Parameters(commandDescription = "profile a dataset")
  class ProfileCommand extends de.hpi.isg.pyro.core.Configuration {

    /**
      * If Pyro should be run in the distributed mode, then this property should include a semicolon separated list of
      * hosts to run on, e.g, `"worker1:35711;worker2:35711;worker3:35711"`, with the first host being the head of
      * the operation (here: `worker1:35711`). Otherwise, this property should not be specified.
      */
    @Parameter(names = Array("--hosts"), description = "list of hosts to run Pyro on, e.g. worker1:123 worker2:123" +
      "first host should be this machine; leave blank for a non-distributed set-up", variableArity = true)
    var hostsDefinition: java.util.List[String] = _

    /**
      * Parses the [[hostsDefinition]].
      *
      * @return an [[Array]] of [[Host]]s
      */
    def hosts: Array[Host] =
      if (hostsDefinition == null || hostsDefinition.isEmpty) Array()
      else hostsDefinition.map(Host.parse).toArray

    @Parameter(description = "path to input file; should be present on all workers", required = true, arity = 1)
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

    // TODO: Metacrate parameters...
  }

  /**
    * Configures for starting a passive worker.
    */
  @Parameters(commandDescription = "start a passive worker")
  class StartWorkerCommand {

    @Parameter(description = "the host and port to bind the worker to, e.g., worker2:123", required = true, arity = 1)
    var hostDefinition: java.util.List[String] = _

    /**
      * Provides the host to bind a worker to.
      * @return the [[Host]]
      */
    def host: Host = Host.parse(hostDefinition(0))
  }

}
