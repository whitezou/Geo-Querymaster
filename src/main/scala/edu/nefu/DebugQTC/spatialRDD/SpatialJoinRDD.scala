package edu.nefu.DebugQTC.spatialRDD

import java.util

import edu.nefu.DebugQTC.buffer_partition.{BufferedPartitioner, TagedGeom}
import edu.nefu.DebugQTC.gqtree.{CellPoint, GQT, GeoObject}
import edu.nefu.DebugQTC.partition.{EqualPartitioning, FlatGridPartitioner, QuadTreePartitioner, SpatialPartitioner}
import edu.nefu.DebugQTC.sample.RDDSampleUtils
import edu.nefu.emus.{GridType, IndexType}
import edu.nefu.quadtree.{QuadtreePartitioning, StandardQuadTree}
import edu.nefu.spatialRddTool.StatCalculator
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.random.SamplingUtils
import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory, Point}
import org.locationtech.jts.index.SpatialIndex
import org.locationtech.jts.index.quadtree.Quadtree
import org.locationtech.jts.index.strtree.{GeometryItemDistance, STRtree}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SpatialJoinRDD() extends Serializable {

  val spatialBuff = new SpatialJoinOpBuff()

  def dataPrepare(sc: SparkContext, pathleft: String, pathRight: String): (RDD[TagedGeom[Geometry]], RDD[(STRtree, GQT)]) = {

    val leftGeometryById = spatialBuff.loadData(sc, pathleft)
    val rightGeometryById = spatialBuff.loadData(sc, pathRight)
    val (leftBufferRDD, rightBufferRDD) = spatialBuff.partitionwithRBuff(sc, leftGeometryById, rightGeometryById)
    val rightIndexRDD = spatialBuff.GQTIndex(sc, rightBufferRDD)

    (leftBufferRDD, rightIndexRDD)
  }

  def spatialJoint(leftBufferRDD: RDD[TagedGeom[Geometry]], rightIndexRDD: RDD[(STRtree, GQT)]): RDD[(Geometry, Geometry)] = {

    val result = leftBufferRDD.zipPartitions(rightIndexRDD) {
      (iter_buff_quad_partition, iter_buffgqt) =>

        val partitionID = TaskContext.getPartitionId()
        val extend = spatialBuff.partitionGrids.get(partitionID)
        val result = new util.ArrayList[(Geometry, Geometry)]

        if (iter_buffgqt.hasNext && iter_buff_quad_partition.hasNext) {
          val buff_gqt = iter_buffgqt.next()
          val rtree: STRtree = buff_gqt._1
          val gqt: GQT = buff_gqt._2

          while (iter_buff_quad_partition.hasNext) {
            val query_gem: TagedGeom[Geometry] = iter_buff_quad_partition.next()
            val query_envelope = query_gem.geometry.getEnvelopeInternal
            val candidates_rtree = rtree.query(query_envelope).asInstanceOf[util.List[Geometry]].asScala
            val candidates_gqt = gqt.rangQuery(query_envelope).map(_.geometry)

            if (query_gem.multiCopy) {

              for (candidate <- candidates_rtree) {
                if (query_gem.geometry.isInstanceOf[Point]) {
                  println(true)
                  result.add((query_gem.geometry, candidate))
                } else {
                  val candidateEnvelope = candidate.getEnvelopeInternal
                  val intersectionEnvelope = candidateEnvelope.intersection(query_envelope)
                  if (extend.covers(new Coordinate(intersectionEnvelope.getMinX, intersectionEnvelope.getMinY))) {
                    if (candidate.intersects(query_gem.geometry)) {
                      result.add((query_gem.geometry, candidate))
                    }
                  }
                }
              }

              for (candidate <- candidates_gqt) {
                if (query_gem.geometry.isInstanceOf[Point]) {
                  result.add((query_gem.geometry, candidate))
                } else {
                  if (candidate.intersects(query_gem.geometry)) {
                    result.add((query_gem.geometry, candidate))
                  }
                }
              }

            } else {
              for (candidate <- candidates_rtree) {
                if (query_gem.geometry.isInstanceOf[Point]) {
                  result.add((query_gem.geometry, candidate))
                } else {
                  if (candidate.intersects(query_gem.geometry)) {
                    result.add((query_gem.geometry, candidate))
                  }
                }
              }
              for (candidate <- candidates_gqt) {

                if (query_gem.geometry.isInstanceOf[Point]) {
                  result.add((query_gem.geometry, candidate))
                } else {
                  if (candidate.intersects(query_gem.geometry)) {
                    result.add((query_gem.geometry, candidate))
                  }
                }
              }
            }
          }
        }
        result.iterator().asScala
    }
    result
  }
}











