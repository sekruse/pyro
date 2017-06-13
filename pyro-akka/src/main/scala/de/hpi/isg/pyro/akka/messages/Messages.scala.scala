package de.hpi.isg.pyro.akka.messages

/**
  * Message that reports ready [[de.hpi.isg.pyro.akka.actors.NodeManager]] instance.
  */
case class NodeManagerReady() {

}

/**
  * Message that asks to profile a certain dataset.
  */
case class StartProfiling(path: String, hosts: Array[(String, Int)]) {

}

/**
  * Message that reports ready [[de.hpi.isg.pyro.akka.actors.Worker]] instance.
  *
  * @param num number of reader instances
  */
case class WorkersReady(num: Int) {

}
