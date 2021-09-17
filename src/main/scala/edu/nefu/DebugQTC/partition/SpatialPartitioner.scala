package edu.nefu.DebugQTC.partition

import java.io.Serializable
import java.util
import java.util.Objects

import edu.nefu.D_utils.DedupParams
import edu.nefu.emus.GridType
import javax.annotation.Nullable
import org.apache.spark.Partitioner
import org.locationtech.jts.geom.{Envelope, Geometry}


abstract class SpatialPartitioner extends Partitioner with Serializable {

  protected var gridType: GridType=null
  protected var grids: util.List[Envelope]=null

  def this(gridType1: GridType, grids1: util.List[Envelope]) {
    this()
    this.gridType = gridType1
    this.grids = Objects.requireNonNull(grids1, "grids")
  }


  /**
   * Given a geometry, returns a list of partitions it overlaps.
   * <p>
   * For points, returns exactly one partition as long as grid type is non-overlapping.
   * For other geometry types or for overlapping grid types, may return multiple partitions.
   */
  @throws[Exception]
  def placeObject[T <: Geometry](spatialObject: T): util.Iterator[Tuple2[Int, T]]

  @Nullable def getDedupParams: DedupParams

  def getGridType: GridType = gridType

  def getGrids: util.List[Envelope] = grids

  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}
