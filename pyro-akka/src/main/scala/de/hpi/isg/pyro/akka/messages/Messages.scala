package de.hpi.isg.pyro.akka.messages

import de.hpi.isg.pyro.akka.utils.Host
import de.hpi.isg.pyro.core.Configuration
import de.hpi.isg.pyro.model.Column
import de.metanome.algorithm_integration.input.RelationalInputGenerator

/**
  * Message that asks to profile a certain dataset.
  *
  * @param configuration  the [[Configuration]] of how to profiler
  * @param inputPath      the path of the file to profile
  * @param inputGenerator an optional [[RelationalInputGenerator]]
  */
case class ProfilingTask(configuration: Configuration,
                         inputPath: String,
                         inputGenerator: Option[RelationalInputGenerator] = None)

/**
  * This message reports the [[Column]]s in a dataset after it has been loaded.
  * @param columns the [[Column]]s
  */
case class ColumnReport(columns: Seq[Column])

/**
  * This message tells to initialize [[de.hpi.isg.pyro.akka.actors.NodeManager]]s at the given hosts.
  *
  * @param hosts pairs of host names and ports; if empty, then a local [[de.hpi.isg.pyro.akka.actors.NodeManager]]
  *              should be initialized
  */
case class NodeManagerInitialization(hosts: Array[Host])


/**
  * Message that communicates the state of a [[de.hpi.isg.pyro.akka.actors.NodeManager]].
  *
  * @param numIdleWorkers an optional number of idle [[de.hpi.isg.pyro.akka.actors.Worker]]s
  */
case class NodeManagerState(numIdleWorkers: Option[Int] = None)