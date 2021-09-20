package edu.nefu.DebugQTC.buffer_partition

import java.io.Serializable
import java.util
import java.util.Objects

import edu.nefu.D_utils.DedupParams
import edu.nefu.DebugQTC.partition.SpatialPartitioner
import edu.nefu.emus.GridType
import edu.nefu.quadtree.{QuadRectangle, StandardQuadTree}
import javax.annotation.Nullable
import org.apache.spark.Partitioner
import org.locationtech.jts.geom.{Envelope, Geometry, Point}

class JoinPartitioner(partitions:util.Map[Integer, Envelope] ) extends Partitioner  with Serializable {

  override def numPartitions: Int = partitions.size()

  override def getPartition(key: Any): Int = key.asInstanceOf[Int]

//  override def equals(other: Any): Boolean = other match {
//    case mypartition: JoinPartitioner =>
//      mypartition.numPartitions == numPartitions
//    case _ =>
//      false
//  }

  override def hashCode: Int = numPartitions.toInt


}

