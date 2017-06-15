package de.hpi.isg.pyro.akka.utils

import java.util.function.Consumer

import scala.language.implicitConversions

/**
  * This object provides adapters between Java and Scala standard library functionality.
  */
object JavaScalaCompatibility {

  /**
    * Converts a Scala function `(T) => _` to a [[java.util.function.Consumer]].
    */
  implicit def scalaFuncToJavaConsumer[T](scalaFunc: (T) => _): java.util.function.Consumer[T] =
    new Consumer[T] {
      override def accept(t: T): Unit = scalaFunc(t)
    }

}
