package edu.nefu.DebugQTC.gqtree

import org.locationtech.jts.geom.Geometry

import scala.collection.mutable

class GeoObject[T <: Geometry](geometry1: T) extends Serializable {
  var geoEnvelope = geometry1.getEnvelopeInternal
  var geometry: T = geometry1
  var gridQueue = new mutable.Queue[(Int, Int)]()
  var distance: Double = Double.MaxValue
}
