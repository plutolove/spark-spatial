package org.apache.spark.sql.catalyst.spatial

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.spatial.shapes.Shape
import org.apache.spark.sql.catalyst.util.{GenericArrayData, ArrayData}

/*
 *   Created by plutolove on 08/02/2018.
 */
private[sql] class ShapeType extends UserDefinedType[Shape] {
  override def sqlType: DataType = ArrayType(ByteType, containsNull = false)

  override def serialize(s: Shape): Any = {
    new GenericArrayData(ShapeSerializer.serialize(s))
  }

  override def userClass: Class[Shape] = classOf[Shape]

  override def deserialize(datum: Any): Shape = {
    datum match {
      case values: ArrayData =>
        ShapeSerializer.deserialize(values.toByteArray)
    }
  }
}

case object ShapeType extends ShapeType
