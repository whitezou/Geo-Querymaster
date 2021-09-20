package edu.nefu.DebugQTC.spatialRDD

import java.util

import edu.nefu.DebugQTC.buffer_partition.{JoinPartitioner, TagedGeom}
import edu.nefu.DebugQTC.gqtree.{CellPoint, GQT}
import edu.nefu.DebugQTC.sample.RDDSampleUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.SamplingUtils
import org.locationtech.jts.geom.{Envelope, Geometry}
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable
import scala.util.Try

case class Wrapped[A](elem: A)(implicit ordering: Ordering[A])
  extends Ordered[Wrapped[A]] with Serializable {
  def compare(that: Wrapped[A]): Int = ordering.compare(this.elem, that.elem)
}

class SpatialJoinOpBuff extends Serializable {

//  var boundaryEnvelope: Envelope = null
  var partitionGrids: util.Map[Integer, Envelope] = new util.HashMap[Integer, Envelope]

  def loadData(sc: SparkContext, path: String): RDD[Geometry] = {
    val leftData = sc.textFile(path, 256)
    val leftGeometryById = leftData.map(x => Try(new WKTReader().read(x))).filter(_.isSuccess).map(x => (x.get))
    leftGeometryById
  }

  def sortTile(sampleData: RDD[MBR], extent: MBR, dimX: Int, dimY: Int): Array[MBR] = {
    val numObjects = sampleData.count()
    val numObjectsPerTile = math.ceil(numObjects.toDouble / (dimX * dimY)).toInt
    val numObjectsPerSlice = numObjectsPerTile * dimY

    //sort by center_x, slice, center_y
    val centroids = sampleData.map(x => ((x.xmin + x.xmax) / 2.0, (x.ymin + x.ymax) / 2.0))
    val objs = centroids.sortByKey(ascending = true).zipWithIndex().map(x => (x._1._1, x._1._2, x._2))
    val objectsSorted = objs.map(x => (Wrapped(x._3 / numObjectsPerSlice, x._2), x))
      .sortByKey(ascending = true).values

    //pack
    val tiles = objectsSorted.zipWithIndex().map(x => (x._2 / numObjectsPerTile,
      (x._1._1, x._1._2, x._1._1, x._1._2, x._2 / numObjectsPerSlice, x._2 / numObjectsPerTile)))
      .reduceByKey((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4, a._5, a._6))
      .values
    //reduce for slice boundaries
    val sliceMap = tiles.map(x => (x._5, (x._1, x._3))).reduceByKey((a, b) => (a._1 min b._1, a._1 max b._1)).collectAsMap()
    //val sliceBoundsBroadcast = sc.broadcast(sliceBounds)
    //val tileMap = tiles.map()
    val tilesLocal = tiles.collect()
    val tileMap = tiles.keyBy(_._6).collectAsMap()
    //fill out tiles as continuous partitions
    val sliceBounds = sliceMap.map(x => x._1 -> (if (x._1 == 0) extent.xmin else (x._2._1 + sliceMap(x._1 - 1)._2) / 2,
      if (x._1 == sliceMap.size - 1) extent.xmax else (x._2._2 + sliceMap(x._1 + 1)._1) / 2))
    val tilesFinal = tilesLocal.map(x => (sliceBounds(x._5)._1,
      if (x._6 == 0 || (x._6 != 0 && x._5 != tileMap(x._6 - 1)._5)) extent.ymin else (x._2 + tileMap(x._6 - 1)._4) / 2,
      sliceBounds(x._5)._2,
      if (x._6 == tileMap.size - 1 || (x._6 != tileMap.size - 1 && x._5 != tileMap(x._6 + 1)._5)) extent.ymax else (x._4 + tileMap(x._6 + 1)._2) / 2
    ))

    tilesFinal.map(x => new MBR(x._1, x._2, x._3, x._4))
  }

  def rtreeQuery(rtree: => Broadcast[STRtree], x: Geometry, r: Double): Array[(Int, TagedGeom[Geometry])] = {
    val rtreeLocal = rtree.value
    val queryEnv = x.getEnvelopeInternal
    queryEnv.expandBy(r)
    var results: Array[(Int, TagedGeom[Geometry])] = null
    val candidates = rtreeLocal.query(queryEnv).toArray
    if (candidates.size > 1) {
      results = candidates.map { case (geom_, id_) => (id_.asInstanceOf[Int], new TagedGeom[Geometry](true, x)) }
    }else{
      results = candidates.map { case (geom_, id_) => (id_.asInstanceOf[Int], new TagedGeom[Geometry](false, x)) }
    }
    results
  }

  def partitionwithRBuff(sc: SparkContext,
                         leftGeometryWithId: RDD[Geometry],
                         rightGeometryWithId: RDD[Geometry]): (RDD[TagedGeom[Geometry]], RDD[ TagedGeom[Geometry]]) = {
    val extent = {
      val temp = leftGeometryWithId.map(x => x.getEnvelopeInternal)
        .map(x => (x.getMinX, x.getMinY, x.getMaxX, x.getMaxY))
        .reduce((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4))
      val temp2 = rightGeometryWithId.map(x => x.getEnvelopeInternal)
        .map(x => (x.getMinX, x.getMinY, x.getMaxX, x.getMaxY))
        .reduce((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4))

      new MBR(temp._1 min temp2._1, temp._2 min temp2._2, temp._3 max temp2._3, temp._4 max temp2._4)
    }
//    this.boundaryEnvelope = extent.toEnvelope
    //    val extent = new MBR(extent_temp._1, extent_temp._2, extent_temp._3, extent_temp._4)
    val dimX = 32
    val dimY = 32
    val ratio = 0.01
    val sampleData = rightGeometryWithId.sample(withReplacement = false, fraction = ratio)
      .map(_.getEnvelopeInternal)
      .map(x => new MBR(x.getMinX, x.getMinY, x.getMaxX, x.getMaxY))
    val partitions = sortTile(sampleData, extent, dimX, dimY).zipWithIndex
    val rtree: STRtree = new STRtree()
    for (i <- partitions) {
      val mbr = new Envelope(i._1.xmin, i._1.xmax, i._1.ymin, i._1.ymax)

      this.partitionGrids.put(i._2, mbr)
      rtree.insert(mbr, i)
    }

    val rtreeBroadcast = sc.broadcast(rtree)

//    var leftPairs: RDD[(Long, TagedGeom[Geometry])] = sc.emptyRDD
//    var rightPairs: RDD[(Long, TagedGeom[Geometry])] = sc.emptyRDD
    val partitioner = new JoinPartitioner(this.partitionGrids)

    var leftPairs = leftGeometryWithId.flatMap(x => rtreeQuery(rtreeBroadcast, x, 0)).partitionBy(partitioner) //.groupByKey()
    var rightPairs = rightGeometryWithId.flatMap(x => rtreeQuery(rtreeBroadcast, x, 0)).partitionBy(partitioner) //.groupByKey()
    val leftResultRDD = leftPairs.map(_._2)
    val rightResultRDD = rightPairs.map(_._2)
    (leftResultRDD, rightResultRDD)
  }


  def GQTIndex(sc: SparkContext, rightGeometryWithId: RDD[TagedGeom[Geometry]]): RDD[(STRtree, GQT)] = {
    val buffer_gqt_indexedRDD = rightGeometryWithId.mapPartitionsWithIndex {
      (index, iter) =>
        val envelope = this.partitionGrids.get(index)
        buffer_gqt_partition_index(iter, envelope)
    }
    buffer_gqt_indexedRDD
  }

  def buffer_gqt_partition_index(objectIterator: Iterator[TagedGeom[Geometry]], envelope: Envelope): Iterator[(STRtree, GQT)] = {
    val left_bot_x = envelope.getMinX
    val left_bot_y = envelope.getMinY
    val right_top_x = envelope.getMaxX
    val right_top_y = envelope.getMaxY
    val array = mutable.Set[Geometry]()
    val gqt = new GQT(1024, 1024, CellPoint(left_bot_x, left_bot_y), CellPoint(right_top_x, right_top_y))
    val strtree = new STRtree
    while (objectIterator.hasNext) {
      val tagedGeom = objectIterator.next()
      if (tagedGeom.multiCopy) {
        val multicopy_geometry = tagedGeom.geometry
        array += tagedGeom.geometry
        strtree.insert(multicopy_geometry.getEnvelopeInternal, multicopy_geometry)
      } else {
        gqt.insertion(tagedGeom.geometry)
      }
    }
    Iterator((strtree, gqt))
  }


}
