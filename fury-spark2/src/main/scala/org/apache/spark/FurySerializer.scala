//  Copyright 2025 Jay Han
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  Copyright 2025 Jay Han
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package org.apache.spark

import org.apache.fury.Fury
import org.apache.fury.config.Language
import org.apache.fury.io.FuryInputStream
import org.apache.fury.memory.MemoryBuffer
import org.apache.spark.scheduler.{CompressedMapStatus, HighlyCompressedMapStatus}
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.storage.{BlockManagerId, StorageLevel}
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.util.{BoundedPriorityQueue, Utils}

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.control.NonFatal

class FurySerializer(conf: SparkConf) extends Serializer with Serializable {
  private val classesToRegister = conf.get("spark.fury.classesToRegister", "")
    .split(',').map(_.trim)
    .filter(_.nonEmpty)

  private val userRegistrators = conf.get("spark.fury.registrator", "")
    .split(',').map(_.trim)
    .filter(!_.isEmpty)

  private[spark] def newFury(): Fury = {
    val fury = Fury
      .builder()
      .withLanguage(Language.JAVA)
      .withScalaOptimizationEnabled(true)
      //      .withMetaCompressor() // TODO, zstd
      .build()

    val oldClassLoader = Thread.currentThread.getContextClassLoader
    val classLoader = defaultClassLoader.getOrElse(Thread.currentThread.getContextClassLoader)

    for (cls <- org.apache.spark.FurySerializer.toRegister) {
      fury.register(cls)
    }

    try {
      // Use the default classloader when calling the user registrator.
      Thread.currentThread.setContextClassLoader(classLoader)
      // Register classes given through spark.fury.classesToRegister.
      classesToRegister
        .foreach { className => fury.register(Class.forName(className, true, classLoader)) }
      userRegistrators
        .map(Class.forName(_, true, classLoader).newInstance().asInstanceOf[FuryRegistrator])
        .foreach { reg => reg.registerClasses(fury) }
    } catch {
      case e: Exception =>
        throw new SparkException(s"Failed to register classes with Fury", e)
    } finally {
      Thread.currentThread.setContextClassLoader(oldClassLoader)
    }

    fury.register(classOf[Array[Tuple1[Any]]])
    fury.register(classOf[Array[Tuple2[Any, Any]]])
    fury.register(classOf[Array[Tuple3[Any, Any, Any]]])
    fury.register(classOf[Array[Tuple4[Any, Any, Any, Any]]])
    fury.register(classOf[Array[Tuple5[Any, Any, Any, Any, Any]]])
    fury.register(classOf[Array[Tuple6[Any, Any, Any, Any, Any, Any]]])
    fury.register(classOf[Array[Tuple7[Any, Any, Any, Any, Any, Any, Any]]])
    fury.register(classOf[Array[Tuple8[Any, Any, Any, Any, Any, Any, Any, Any]]])
    fury.register(classOf[Array[Tuple9[Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    fury.register(classOf[Array[Tuple10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    fury.register(classOf[Array[Tuple11[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    fury.register(classOf[Array[Tuple12[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    fury.register(classOf[Array[Tuple13[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    fury.register(classOf[Array[Tuple14[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    fury.register(classOf[Array[Tuple15[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    fury.register(classOf[Array[Tuple16[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    fury.register(classOf[Array[Tuple17[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    fury.register(classOf[Array[Tuple18[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    fury.register(classOf[Array[Tuple19[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    fury.register(classOf[Array[Tuple20[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    fury.register(classOf[Array[Tuple21[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    fury.register(classOf[Array[Tuple22[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])

    fury.register(None.getClass)
    fury.register(Nil.getClass)
    fury.register(Utils.classForName("scala.collection.immutable.$colon$colon"))
    fury.register(Utils.classForName("scala.collection.immutable.Map$EmptyMap$"))
    fury.register(classOf[ArrayBuffer[Any]])

    // We can't load those class directly in order to avoid unnecessary jar dependencies.
    // We load them safely, ignore it if the class not found.
    Seq(
      "org.apache.spark.sql.catalyst.expressions.UnsafeRow",
      "org.apache.spark.sql.catalyst.expressions.UnsafeArrayData",
      "org.apache.spark.sql.catalyst.expressions.UnsafeMapData",

      "org.apache.spark.ml.feature.Instance",
      "org.apache.spark.ml.feature.LabeledPoint",
      "org.apache.spark.ml.feature.OffsetInstance",
      "org.apache.spark.ml.linalg.DenseMatrix",
      "org.apache.spark.ml.linalg.DenseVector",
      "org.apache.spark.ml.linalg.Matrix",
      "org.apache.spark.ml.linalg.SparseMatrix",
      "org.apache.spark.ml.linalg.SparseVector",
      "org.apache.spark.ml.linalg.Vector",
      "org.apache.spark.ml.tree.impl.TreePoint",
      "org.apache.spark.mllib.clustering.VectorWithNorm",
      "org.apache.spark.mllib.linalg.DenseMatrix",
      "org.apache.spark.mllib.linalg.DenseVector",
      "org.apache.spark.mllib.linalg.Matrix",
      "org.apache.spark.mllib.linalg.SparseMatrix",
      "org.apache.spark.mllib.linalg.SparseVector",
      "org.apache.spark.mllib.linalg.Vector",
      "org.apache.spark.mllib.regression.LabeledPoint"
    ).foreach { name =>
      try {
        val clazz = Utils.classForName(name)
        fury.register(clazz)
      } catch {
        case NonFatal(_) => // do nothing
        case _: NoClassDefFoundError if Utils.isTesting => // See SPARK-23422.
      }
    }
    fury
  }

  override def newInstance(): SerializerInstance = {
    new FurySerializerInstance(this)
  }

}

trait FuryRegistrator {
  def registerClasses(fury: Fury): Unit
}

object FurySerializer {
  // Commonly used classes.
  val toRegister: Seq[Class[_]] = Seq(
    ByteBuffer.allocate(1).getClass,
    classOf[StorageLevel],
    classOf[CompressedMapStatus],
    classOf[HighlyCompressedMapStatus],
    classOf[CompactBuffer[_]],
    classOf[BlockManagerId],
    classOf[Array[Boolean]],
    classOf[Array[Byte]],
    classOf[Array[Short]],
    classOf[Array[Int]],
    classOf[Array[Long]],
    classOf[Array[Float]],
    classOf[Array[Double]],
    classOf[Array[Char]],
    classOf[Array[String]],
    classOf[Array[Array[String]]],
    classOf[BoundedPriorityQueue[_]],
    classOf[SparkConf]
  )
}

class FurySerializerInstance(serializer: FurySerializer) extends SerializerInstance {
  private[spark] val fury = serializer.newFury()
  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    ByteBuffer.wrap(fury.serialize(t))
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    fury.deserialize(MemoryBuffer.fromByteBuffer(bytes)).asInstanceOf[T]
  }

  override def deserialize[T: ClassTag](
                                         bytes: ByteBuffer,
                                         loader: ClassLoader
                                       ): T = {
    val oldClassLoader = Thread.currentThread().getContextClassLoader
    Thread.currentThread().setContextClassLoader(loader)
    try {
      deserialize(bytes)
    } finally {
      Thread.currentThread().setContextClassLoader(oldClassLoader)
    }
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new FurySerializationStream(this, s)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new FuryDeserializationStream(this, new FuryInputStream(s))
  }
}

class FurySerializationStream(serInstance: FurySerializerInstance,
                              output: OutputStream) extends SerializationStream {
  private val fury: Fury = serInstance.fury

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    fury.serialize(output, t)
    this
  }

  override def flush() {
    output.flush()
  }

  override def close() {
    output.close()
  }
}

class FuryDeserializationStream(serInstance: FurySerializerInstance,
                                input: FuryInputStream) extends DeserializationStream {

  override def readObject[T: ClassTag](): T = {
    serInstance.fury.deserialize(input).asInstanceOf[T]
  }

  override def close(): Unit = {
    input.close()
  }
}
