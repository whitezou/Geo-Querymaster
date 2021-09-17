package edu.nefu.DebugQTC.buffer_partition

import org.locationtech.jts.geom.Geometry

class TagedGeom[T <: Geometry](multiCopy1: Boolean,geomtry1: T) extends Serializable {
  var multiCopy: Boolean = multiCopy1
  var geometry = geomtry1

}
