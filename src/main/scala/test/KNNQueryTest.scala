package test

import edu.nefu.DebugQTC.spatialRDD.SpatialRDD
import edu.nefu.DebugQTC.utils.SimpleLoader
import edu.nefu.emus.{FileDataSplitter, IndexType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.random
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.{Coordinate, Envelope, GeometryFactory}

object KNNQueryTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("GQT-KNNQuery")
//      .setMaster("local[1]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val points = "hdfs://master:9000/data/World/points_80M_wkt.csv"
    val polygons = "hdfs://master:9000/data/World/buildings_80M.csv"
    val linestrings = "hdfs://master:9000/data/World/linestrings_72M.csv"
    spatialKNNQuery(sc, points, "point")
    spatialKNNQuery(sc, linestrings, "line")
    spatialKNNQuery(sc, polygons, "polygon")
  }

  def spatialKNNQuery(sc: SparkContext, pointPath: String, query_type: String): Unit = {
    if ("point".equals(query_type)) {
      println("************************ POINT Range Queries **************************************")
    }
    else if ("line".equals(query_type)) {
      println("************************ LineString Range Queries **************************************")
    }
    else if ("polygon".equals(query_type)) {
      println("************************ POLYGON Range Queries **************************************")
    }


    val nQueries = 100
    val random = scala.util.Random
    val geometryFactory = new GeometryFactory()

    val loader = new SimpleLoader
    val geomRDD = loader.load(sc, pointPath, FileDataSplitter.WKT)
    val transRDD = new SpatialRDD(geomRDD)

//    transRDD.origionRDD.persist(StorageLevel.MEMORY_ONLY)
    transRDD.build_gqt_Index(false)
    transRDD.gqt_indexedRDD.persist(StorageLevel.MEMORY_ONLY)

//    transRDD.origionRDD.unpersist()

    val rangeQueryWindow6 = new Envelope(-180.0, 180.0, -90.0, 90.0)
    //    t0 = System.nanoTime()
    for (i <- 1 to 20) {
      transRDD.rangeQuery(rangeQueryWindow6, IndexType.GQT).count()
    }


    println("k=1")
    var t0 = 0L
    var t1 = 0L
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      transRDD.knnQuery(kNNQueryPoint, 1, true)
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=5")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      transRDD.knnQuery(kNNQueryPoint, 5, true)
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=10")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      transRDD.knnQuery(kNNQueryPoint, 10, true)
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=20")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      transRDD.knnQuery(kNNQueryPoint, 20, true)
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=30")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      transRDD.knnQuery(kNNQueryPoint, 30, true)
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=40")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      transRDD.knnQuery(kNNQueryPoint, 40, true)
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=50")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      transRDD.knnQuery(kNNQueryPoint, 40, true)
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")


    transRDD.gqt_indexedRDD.unpersist()


    println("***********************************************************************************\n\n")

  }
}
