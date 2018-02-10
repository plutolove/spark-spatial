package org.apache.spark.sql.catalyst

import org.scalatest.FunSuite
import org.apache.spark.sql.catalyst.spatial.shapes._
import org.apache.spark.sql.catalyst.spatial.ShapeSerializer

/*
 *   Created by plutolove on 10/02/2018.
 */
class PointSuite extends FunSuite {
  test("serialize") {
    val p = Point(Array(1.0, 2.0))
    val p1 = Point(Array(1.0, 2.0))
    val bytes = ShapeSerializer.serialize(p)
    val bp1 = ShapeSerializer.serialize(p1)
    assert(bytes sameElements  bp1)
  }

  test("Deserialize") {
    val p = Point(Array(1.0, 2.0))
    val bytes = ShapeSerializer.serialize(p)
    val accept = ShapeSerializer.deserialize(bytes)
    accept match {
      case poi: Point =>
        assert(poi == p)
      case _ =>
        assert(false)
    }
  }
  val p1 = Point(Array(0.0, 0.0))
  val p2 = Point(Array(1.0, 1.0))
  val p3 = Point(Array(1.0, 1.0))
  test("minDist"){
    assert(Math.abs(p1.minDist(p2) - Math.sqrt(2.0)) < 1e-8)
    assert(p2.minDist(p3) == 0.0)
  }
  test("equals"){
    assert(!(p1 == p2))
    assert(p2 == p3)
  }
  test("less"){
    assert(p1 <= p2)
    assert(p2 <= p3)
    assert(!(p2  <= p1))
  }
}
