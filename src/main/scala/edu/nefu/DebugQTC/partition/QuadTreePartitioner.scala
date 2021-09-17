package edu.nefu.DebugQTC.partition

import java.util
import java.util.Objects

import edu.nefu.D_utils.{DedupParams, HalfOpenRectangle}
import edu.nefu.emus.GridType
import edu.nefu.quadtree.{QuadRectangle, StandardQuadTree}
import javax.annotation.Nullable
import org.locationtech.jts.geom.{Envelope, Geometry, Point}


class QuadTreePartitioner(quadTree1: StandardQuadTree[_ <: Geometry]) extends SpatialPartitioner {

  private var quadTree: StandardQuadTree[_ <: Geometry] = quadTree1

  // Make sure not to broadcast all the samples used to build the Quad
  // tree to all nodes which are doing partitioning

  this.quadTree.dropElements()
  grids = getLeafGrids(quadTree)
  gridType = GridType.QUADTREE

  //  def this (quadTree: StandardQuadTree[_ <: Geometry]){
  //    this()
  //    this.quadTree = quadTree
  //
  //    // Make sure not to broadcast all the samples used to build the Quad
  //    // tree to all nodes which are doing partitioning
  //    this.quadTree.dropElements()
  //  }
  //  this.quadTree = quadTree
  // Make sure not to broadcast all the samples used to build the Quad
  // tree to all nodes which are doing partitioning

  this.quadTree.dropElements()

  @throws[Exception]
  override def placeObject[T <: Geometry](spatialObject: T): util.Iterator[(Int, T)] = {

    Objects.requireNonNull(spatialObject, "spatialObject")

    val envelope = spatialObject.getEnvelopeInternal

    val matchedPartitions = quadTree.findZones(new QuadRectangle(envelope))

    val point = if (spatialObject.isInstanceOf[Point]) spatialObject.asInstanceOf[Point]
    else null

    val result = new util.HashSet[(Int, T)]

    import scala.collection.JavaConversions._

    for (rectangle <- matchedPartitions) { // For points, make sure to return only one partition
      if (point != null && !(new HalfOpenRectangle(rectangle.getEnvelope)).contains(point)) {

      } else {
        result.add((rectangle.partitionId, spatialObject))
      }

    }
    result.iterator
  }

  @Nullable override def getDedupParams = new DedupParams(grids)

  override def numPartitions: Int = grids.size

  override def equals(o: Any): Boolean = {
    if (o == null || !o.isInstanceOf[QuadTreePartitioner]) return false
    val other = o.asInstanceOf[QuadTreePartitioner]
    other.quadTree == this.quadTree
  }

  private def getLeafGrids(quadTree: StandardQuadTree[_ <: Geometry]) = {
    Objects.requireNonNull(quadTree, "quadTree")
    val zones = quadTree.getLeafZones
    val grids = new util.ArrayList[Envelope]

    import scala.collection.JavaConversions._
    for (zone <- zones) {
      grids.add(zone.getEnvelope)
    }
    grids
  }


}

