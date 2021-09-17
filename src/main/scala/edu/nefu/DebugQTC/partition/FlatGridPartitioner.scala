package edu.nefu.DebugQTC.partition

import java.util
import java.util.Objects

import edu.nefu.D_utils.DedupParams
import edu.nefu.emus.GridType
import javax.annotation.Nullable
import org.locationtech.jts.geom.{Envelope, Geometry}

class FlatGridPartitioner(gridType1: GridType, grids1: util.List[Envelope]) extends SpatialPartitioner(gridType1: GridType, grids1: util.List[Envelope]) {
  def this(grids: util.List[Envelope]) {
    this(null, grids)
  }

  /**
   * Given a geometry, returns a list of partitions it overlaps.
   * <p>
   * For points, returns exactly one partition as long as grid type is non-overlapping.
   * For other geometry types or for overlapping grid types, may return multiple partitions.
   */
  override def placeObject[T <: Geometry](spatialObject: T): util.Iterator[(Int, T)] = {

    Objects.requireNonNull(spatialObject, "spatialObject")
    // Some grid types (RTree and Voronoi) don't provide full coverage of the RDD extent and
    // require an overflow container.
    val overflowContainerID = grids.size

    val envelope = spatialObject.getEnvelopeInternal

    val result = new util.HashSet[(Int, T)]
    var containFlag = false
    for (i <- 0 to (grids.size-1)) {
      val grid = grids.get(i)
      if (grid.covers(envelope)) {
        result.add((i, spatialObject))
        containFlag = true
      }
      else if (grid.intersects(envelope) || envelope.covers(grid)) {
        result.add((i, spatialObject))
        containFlag = true
      }
    }
    if (!containFlag) result.add((overflowContainerID, spatialObject))
    result.iterator


  }

  @Nullable override def getDedupParams: DedupParams = {
    /**
     * Equal and Hilbert partitioning methods have necessary properties to support de-dup.
     * These methods provide non-overlapping partition extents and not require overflow
     * partition as they cover full extent of the RDD. However, legacy
     * SpatialRDD.spatialPartitioning(otherGrids) method doesn't preserve the grid type
     * making it impossible to reliably detect whether partitioning allows efficient de-dup or not.
     *
     * TODO Figure out how to remove SpatialRDD.spatialPartitioning(otherGrids) API. Perhaps,
     * make the implementation no-op and fold the logic into JoinQuery, RangeQuery and KNNQuery APIs.
     */
    null
  }

  override def numPartitions: Int = {
    grids.size + 1
  }

  /* overflow partition */
  override def equals(o: Any): Boolean = {
    if (o == null || !o.isInstanceOf[FlatGridPartitioner]) {
      false
    }
    val other = o.asInstanceOf[FlatGridPartitioner]
    // For backwards compatibility (see SpatialRDD.spatialPartitioning(otherGrids))

    if (this.gridType == null || other.gridType == null) {
      other.grids == this.grids
    }

    other.gridType.equals(this.gridType) && other.grids.equals(this.grids)
  }
}
