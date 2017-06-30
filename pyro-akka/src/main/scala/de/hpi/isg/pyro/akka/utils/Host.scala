package de.hpi.isg.pyro.akka.utils

/**
  * This class describes a host in terms of its host name and port.
  */
case class Host(hostName: String, port: Int = Host.defaultPort)

/**
  * Companion object.
  */
object Host {

  private val hostPort = """(.+):(\d+)""".r

  val defaultPort = 7976

  /**
    * Parses a host specification.
    *
    * @param spec the specification with the pattern `hostname:port`
    * @return the [[Host]]
    */
  def parse(spec: String): Host = spec match {
    case hostPort(hostName, port) => Host(hostName, port.toInt)
    case hostName => Host(hostName)
  }

  /**
    * Tries to provide a local [[Host]]. Note that this is ambiguous.
    *
    * @return the local [[Host]]
    */
  def localhost(port: Int = defaultPort): Host = Host(java.net.InetAddress.getLocalHost.getHostName, port)

}