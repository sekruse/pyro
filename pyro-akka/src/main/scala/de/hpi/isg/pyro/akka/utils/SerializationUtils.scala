package de.hpi.isg.pyro.akka.utils

import com.esotericsoftware.kryo.serializers.FieldSerializer
import com.twitter.chill.{KryoBase, ScalaKryoInstantiator}
import de.hpi.isg.pyro.util.{SynchronizedVerticalMap, VerticalMap}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Utilities for serialization.
  */
object SerializationUtils {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Clears the closure of the given object by setting the `$outer` property to `null`.
    *
    * @param element the element whose closure is to be cleared
    * @return the element
    */
  def clearClosure[T](element: T): T = {
    val cls = element.getClass
    Try(cls.getDeclaredField("$outer")) match {
      case Failure(_) =>
        logger.warn(s"$element of type ${element.getClass} does not seem to have a closure.")
      case Success(field) =>
        field.setAccessible(true)
        field.set(element, null)
    }
    element
  }

  /**
    * [[com.twitter.chill.KryoInstantiator]] that we use to configure Kryo to Pyro's needs.
    */
  object PyroKryoInstantiator extends ScalaKryoInstantiator {
    override def newKryo(): KryoBase = {
      val kryo = super.newKryo()

      // VerticalMaps do not go together with the MapSerializer... serialize them as normal objects.
      kryo.addDefaultSerializer(
        classOf[VerticalMap[_]],
        classOf[FieldSerializer[_]]
      )
      kryo.addDefaultSerializer(
        classOf[SynchronizedVerticalMap[_]],
        classOf[FieldSerializer[_]]
      )
      kryo
    }
  }

}
