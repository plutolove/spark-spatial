package org.apache.spark.sql.catalyst.spatial

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import java.io._
import org.apache.spark.sql.catalyst.spatial.shapes._
import org.apache.spark.sql.catalyst.spatial.util.KryoSerializer

/*
 *   Created by plutolove on 08/02/2018.
 */
object ShapeSerializer {
  private[sql] val kryo = new Kryo()
  kryo.register(classOf[Shape], new KryoSerializer)
  kryo.register(classOf[Point], new KryoSerializer)
  kryo.addDefaultSerializer(classOf[Shape], new KryoSerializer)
  kryo.setReferences(false)

  def deserialize(data: Array[Byte]): Shape = {
    val in = new ByteArrayInputStream(data)
    val input = new Input(in)
    val res = kryo.readObject(input, classOf[Shape])
    input.close()
    res
  }

  def serialize(o: Shape): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val output = new Output(out)
    kryo.writeObject(output, o)
    output.close()
    out.toByteArray
  }
}