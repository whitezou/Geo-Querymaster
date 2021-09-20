package edu.nefu.DebugQTC.spatialRDD

object SpatialOperator extends Enumeration {
  type SpatialOperator = Value
  val Within, WithinD, Contains, Intersects, Overlaps, NearestD, NA = Value

}
