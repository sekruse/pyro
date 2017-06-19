package de.hpi.isg.pyro.akka.utils

/**
  * This class describes a host in terms of its host name and port.
  */
case class Host(hostName: String, port: Int)

/**
  * Companion object.
  */
object Host {

  /**
    * Parses a host specification.
    *
    * @param spec the specification with the pattern `hostname:port`
    * @return the [[Host]]
    */
  def parse(spec: String): Host = {
    val Array(hostName, port) = spec.split(":")
    Host(hostName, port.toInt)
  }

}