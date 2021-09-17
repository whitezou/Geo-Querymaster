package test

import edu.nefu.DebugQTC.spatialRDD.SpatialRDD
import edu.nefu.DebugQTC.utils.SimpleLoader
import edu.nefu.emus.{FileDataSplitter, GridType, IndexType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Envelope

object RangeQueryTest2 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("GQT-RangeQuery")
//      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val points = "hdfs://master:9000/data/World/points_80M_wkt.csv"
//        val points = "/home/runxuan/data/Spatial_Compare/World/single_machine/points_10M_wkt.csv"
    val polygons = "hdfs://master:9000/data/World/buildings_80M.csv"
    val linestrings = "hdfs://master:9000/data/World/linestrings_72M.csv"

    spatialRangeQuery(sc, points, "point")
    spatialRangeQuery(sc, linestrings, "line")
    spatialRangeQuery(sc, polygons, "polygon")
  }


  def spatialRangeQuery(sc: SparkContext, pointPath: String, query_type: String): Unit = {
    val rangeQueryWindow1 = new Envelope(-50.3010141441, -24.9526465797, -53.209588996, -30.1096863746)
    val rangeQueryWindow2 = new Envelope(-54.4270741441, -24.9526465797, -53.209588996, -30.1096863746)
    val rangeQueryWindow3 = new Envelope(-114.4270741441, 42.9526465797, -54.509588996, -27.0106863746)
    val rangeQueryWindow4 = new Envelope(-82.7638020000, 42.9526465797, -54.509588996, 38.0106863746)
    val rangeQueryWindow5 = new Envelope(-140.99778, 5.7305630159, -52.6480987209, 83.23324)
    val rangeQueryWindow6 = new Envelope(-180.0, 180.0, -90.0, 90.0)
    val loader = new SimpleLoader
    //        val geomRDD = loader.load(sc, "/home/runxuan/data/Spatial_Compare/1points_10M_wkt.csv", FileDataSplitter.WKT)
    //    val geomRDD = loader.load(sc, "/home/runxuan/data/transform_trim/OSM_trans/sports.csv", FileDataSplitter.WKT)
    val geomRDD = loader.load(sc, pointPath, FileDataSplitter.WKT)
    val transRDD = new SpatialRDD(geomRDD)
    //    transRDD.origionRDD.persist(StorageLevel.MEMORY_ONLY)
    transRDD.partition(GridType.BUFFEREDTREE)

    transRDD.buffer_gqt_Index()

    transRDD.buffer_gqt_indexedRDD.persist(StorageLevel.MEMORY_ONLY)
    //    transRDD.build_simple_Index(IndexType.RTREE, false)
    //    transRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
    var t0 = 0L
    var t1 = 0L
    var count1 = 0L
    var count = 0L
    val nQueries = 100

    if ("point".equals(query_type)) {

      println("************************ POINT Range Queries **************************************")
    }
    else if ("line".equals(query_type)) {
      println("************************ LineString Range Queries **************************************")
    }
    else if ("polygon".equals(query_type)) {
      println("************************ POLYGON Range Queries **************************************")
    }

    //    t0 = System.nanoTime()
    for (i <- 1 to 20) {
      count = transRDD.rangeQuery_onbufferpartition(rangeQueryWindow6, IndexType.GQT).count()
      //            count = transRDD.rangeQuery(rangeQueryWindow6, IndexType.RTREE).count()
    }


    println("Range1: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1  = transRDD.rangeQuery_onbufferpartition(rangeQueryWindow1, IndexType.GQT).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    println("count:" + count)
    t1 = 0L
    t0 = 0L

    println("Range2: ")

    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1  = transRDD.rangeQuery_onbufferpartition(rangeQueryWindow2, IndexType.GQT).count()
    }

    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    println("count:" + count)
    t1 = 0L
    t0 = 0L

    println("Range3: ")

    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1  = transRDD.rangeQuery_onbufferpartition(rangeQueryWindow3, IndexType.GQT).count()
    }

    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    println("count:" + count)
    t1 = 0L
    t0 = 0L
    println("Range4: ")

    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1  = transRDD.rangeQuery_onbufferpartition(rangeQueryWindow4, IndexType.GQT).count()
    }

    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    println("count:" + count)
    t1 = 0L
    t0 = 0L

    println("Range5: ")

    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1  = transRDD.rangeQuery_onbufferpartition(rangeQueryWindow5, IndexType.GQT).count()
    }

    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    println("count:" + count)
    t1 = 0L
    t0 = 0L

    println("Range6: ")

    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1  = transRDD.rangeQuery_onbufferpartition(rangeQueryWindow6, IndexType.GQT).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    println("count:" + count)
    t1 = 0L
    t0 = 0L

    transRDD.buffer_gqt_indexedRDD.unpersist()
    //    transRDD.indexedRDD.unpersist()
    println("***********************************************************************************\n\n")

  }

}
