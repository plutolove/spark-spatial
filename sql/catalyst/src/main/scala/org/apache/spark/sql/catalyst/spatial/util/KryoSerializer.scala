package org.apache.spark.sql.catalyst.spatial.util

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.spark.sql.catalyst.spatial.shapes.{Shape, Point}

/*
 *   Created by plutolove on 08/02/2018.
 */
class KryoSerializer extends Serializer[Shape] {
  private def getTypeInt(o: Shape): Short = o match {
    case _: Point => 0
  }

  override def write(kryo: Kryo, output: Output, shape: Shape): Unit = {
    output.writeShort(getTypeInt(shape))
    shape match {
      case p: Point =>
        output.writeInt(p.dimensions, true)
        p.coord.foreach(output.writeDouble)
    }
  }

  override def read(kryo: Kryo, input: Input, tp: Class[Shape]): Shape = {
    val type_int = input.readShort()
    if (type_int == 0) {
      val dim = input.readInt(true)
      val coords = Array.ofDim[Double](dim)
      for (i <- 0 until dim) coords(i) = input.readDouble()
      Point(coords)
    }else null
  }
}