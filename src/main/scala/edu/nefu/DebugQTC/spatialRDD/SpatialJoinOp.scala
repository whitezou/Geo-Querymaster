package edu.nefu.DebugQTC.spatialRDD

import edu.nefu.DebugQTC.spatialRDD.SpatialOperator.SpatialOperator
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Envelope, Geometry}
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class SpatialJoinOp extends Serializable {


  def loadData(sc: SparkContext, path: String): RDD[(Long, Geometry)] = {
    val leftData = sc.textFile(path, 256).map(x => x.split("\t")).zipWithIndex()
    val leftGeometryById = leftData.map(x => (x._2, Try(new WKTReader().read(x._1.apply(0))))).filter(_._2.isSuccess).map(x => (x._1, x._2.get)).cache()
    leftGeometryById
  }


  def partitionwithRBuff(sc: SparkContext,
                         leftGeometryWithId: RDD[(Long, Geometry)],
                         rightGeometryWithId: RDD[(Long, Geometry)]): (RDD[(Long, Iterable[(Long, Geometry)])], RDD[(Long, Iterable[(Long, Geometry)])]) = {
    val extent = {
      val temp = leftGeometryWithId.map(x => x._2.getEnvelopeInternal)
        .map(x => (x.getMinX, x.getMinY, x.getMaxX, x.getMaxY))
        .reduce((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4))

      val temp2 = rightGeometryWithId.map(x => x._2.getEnvelopeInternal)
        .map(x => (x.getMinX, x.getMinY, x.getMaxX, x.getMaxY))
        .reduce((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4))

      new MBR(temp._1 min temp2._1, temp._2 min temp2._2, temp._3 max temp2._3, temp._4 max temp2._4)
    }
    //    val extent = new MBR(extent_temp._1, extent_temp._2, extent_temp._3, extent_temp._4)
    val dimX = 32
    val dimY = 32
    val ratio = 0.3
    val sampleData = rightGeometryWithId.sample(withReplacement = false, fraction = ratio)
      .map(_._2.getEnvelopeInternal)
      .map(x => new MBR(x.getMinX, x.getMinY, x.getMaxX, x.getMaxY))
    val partitions = sortTile(sampleData, extent, dimX, dimY).zipWithIndex

    val rtree: STRtree = new STRtree()
    for (i <- partitions) {
      val mbr = new Envelope(i._1.xmin, i._1.xmax, i._1.ymin, i._1.ymax)
      rtree.insert(mbr, i)
    }

    val rtreeBroadcast = sc.broadcast(rtree)

    var leftPairs: RDD[(Long, Iterable[(Long, Geometry)])] = sc.emptyRDD
    var rightPairs: RDD[(Long, Iterable[(Long, Geometry)])] = sc.emptyRDD
    leftPairs = leftGeometryWithId.flatMap(x => rtreeQuery(rtreeBroadcast, x, 0)).groupByKey()
    rightPairs = rightGeometryWithId.flatMap(x => rtreeQuery(rtreeBroadcast, x, 0)).groupByKey()
    (leftPairs, rightPairs)

  }

  def joinQuery(leftPairs: RDD[(Long, Iterable[(Long, Geometry)])],
                rightPairs: RDD[(Long, Iterable[(Long, Geometry)])]): RDD[(Long, Long)] = {
    val radius = 0
    val joinPredicate = SpatialOperator.Intersects
    val joinedPairs = leftPairs.leftOuterJoin(rightPairs).flatMap(x =>
      localJoinWithinRtree(x._2._1, x._2._2.getOrElse(Iterable.empty[(Long, Geometry)]), joinPredicate, radius))
      .distinct()
    joinedPairs
  }

  def rtreeQuery(rtree: => Broadcast[STRtree], x: (Long, Geometry), r: Double): Array[(Long, (Long, Geometry))] = {
    val rtreeLocal = rtree.value
    val queryEnv = x._2.getEnvelopeInternal
    queryEnv.expandBy(r)
    val candidates = rtreeLocal.query(queryEnv).toArray
    val results = candidates.map { case (geom_, id_) => (id_.asInstanceOf[Int].toLong, x) }
    results
  }

  case class Wrapped[A](elem: A)(implicit ordering: Ordering[A])
    extends Ordered[Wrapped[A]] with Serializable {
    def compare(that: Wrapped[A]): Int = ordering.compare(this.elem, that.elem)
  }


  def sortTile(sampleData: RDD[MBR], extent: MBR, dimX: Int, dimY: Int): Array[MBR] = {
    val numObjects = sampleData.count()
    val numObjectsPerTile = math.ceil(numObjects.toDouble / (dimX * dimY)).toLong
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

  def localJoinWithinRtree(x: Iterable[(Long, Geometry)], y: Iterable[(Long, Geometry)], predicate: SpatialOperator,
                           r: Double = 0.0): Array[(Long, Long)] = {
    val results: ArrayBuffer[(Long, Long)] = new ArrayBuffer[(Long, Long)]()
    val rtree = new STRtree()
    for (i <- y) {
      val mbr = i._2.getEnvelopeInternal
      rtree.insert(mbr, i)
    }
    for (i <- x) {
      val mbr = i._2.getEnvelopeInternal
      mbr.expandBy(r)
      val queryResults = rtree.query(mbr).toArray
      for (j <- queryResults) {
        val obj = j.asInstanceOf[(Long, Geometry)]
        if (predicate == SpatialOperator.Within) {
          if (obj._2.contains(i._2)) results.append((i._1, obj._1))
        }
        else if (predicate == SpatialOperator.Contains) {
          if (i._2.contains(obj._2)) results.append((i._1, obj._1))
        }
        else if (predicate == SpatialOperator.Intersects) {
          if (i._2.intersects(obj._2)) results.append((i._1, obj._1))
        }
        else if (predicate == SpatialOperator.Overlaps) {
          if (i._2.overlaps(obj._2)) results.append((i._1, obj._1))
        }
        else if (predicate == SpatialOperator.WithinD) {
          if (i._2.isWithinDistance(obj._2, r)) results.append((i._1, obj._1))
        }
      }
    }
    results.toArray
  }

}
