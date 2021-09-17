package edu.nefu.DebugQTC.buffer_partition

import java.util
import java.util.{List, Objects}

import edu.nefu.D_utils.{DedupParams, HalfOpenRectangle}
import edu.nefu.DebugQTC.partition.SpatialPartitioner
import edu.nefu.emus.GridType
import edu.nefu.quadtree.{QuadRectangle, StandardQuadTree}
import javax.annotation.Nullable
import org.locationtech.jts.geom.{Envelope, Geometry, Point}

class BufferedPartitioner(quadTree1: StandardQuadTree[_ <: Geometry]) extends SpatialPartitioner {

  private var quadTree: StandardQuadTree[_ <: Geometry] = quadTree1
  def getQuadTree(): StandardQuadTree[_ <: Geometry]={
    this.quadTree
  }

  this.quadTree.dropElements()
  grids = getLeafGrids(quadTree)
  gridType = GridType.QUADTREE
  var partitionsize = grids.size()
  this.quadTree.dropElements()

  @throws[Exception]
  def placegeom[T <: Geometry](spatialObject: T): util.Iterator[(Int, TagedGeom[T])] = {

    Objects.requireNonNull(spatialObject, "spatialObject")

    val envelope = spatialObject.getEnvelopeInternal

    val matchedPartitions = quadTree.findZones(new QuadRectangle(envelope))

    val point = if (spatialObject.isInstanceOf[Point]) spatialObject.asInstanceOf[Point]
    else null

    val result = new util.HashSet[(Int, TagedGeom[T])]

    import scala.collection.JavaConversions._

    val tag = if (matchedPartitions.size() == 1) false
    else true

    for (rectangle <- matchedPartitions) { // For points, make sure to return only one partition
      //      if (point != null && !(new HalfOpenRectangle(rectangle.getEnvelope)).contains(point)) {
      //
      //      } else {
      result.add((rectangle.partitionId, new TagedGeom(tag, spatialObject)))
      //      }

    }
    result.iterator
  }

  @Nullable override def getDedupParams = new DedupParams(grids)

  override def numPartitions: Int = grids.size

  override def equals(o: Any): Boolean = {
    if (o == null || !o.isInstanceOf[BufferedPartitioner]) return false
    val other = o.asInstanceOf[BufferedPartitioner]
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

  /**
   * Given a geometry, returns a list of partitions it overlaps.
   * <p>
   * For points, returns exactly one partition as long as grid type is non-overlapping.
   * For other geometry types or for overlapping grid types, may return multiple partitions.
   */
  override def placeObject[T <: Geometry](spatialObject: T): util.Iterator[(Int, T)] = ???

}

