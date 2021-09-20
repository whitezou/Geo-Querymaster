package test

import edu.nefu.DebugQTC.spatialRDD.{SpatialJoinOp_Geom, SpatialJoinRDD}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SpatialJoinTest4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GQT-SpatialJoin")
    //          .setMaster("local[*]")

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")

    //        val points = "/home/runxuan/data/Spatial_Compare/World/single_machine/points_2M_wkt.csv"
    //        val polygons = "/home/runxuan/data/Spatial_Compare/World/single_machine/buildings_2M.csv"
    //        val rectangles = "/home/runxuan/data/Spatial_Compare/World/single_machine/rectangles_2M.csv"
    //        val linestrings = "/home/runxuan/data/Spatial_Compare/World/single_machine/linestrings_2M.csv"
    //
    val points = "hdfs://master:9000/data/World/points_40M_wkt.csv"
    val rectangles = "hdfs://master:9000/data/World/rectangles_40M.csv"
    val polygons = "hdfs://master:9000/data/World/buildings_40M.csv"
    val linestrings = "hdfs://master:9000/data/World/linestrings_40M.csv"

    println("****************************** " + "points" + " and " + "points" + " spatial join ********************************")
    spatialJoinFun(sc, points, points)
    println("******************************** " + "points" + " and " + "linestrings" + " spatial join ********************************")
    spatialJoinFun(sc, points, linestrings)
    println("******************************** " + "points" + " and " + "polygons" + " spatial join ********************************")
    spatialJoinFun(sc, points, polygons)
    println("******************************** " + "points" + " and " + "rectangles" + " spatial join ********************************")
    spatialJoinFun(sc, points, rectangles)
    println("******************************** " + "linestrings" + " and " + "linestrings" + " spatial join ********************************")
    spatialJoinFun(sc, linestrings, linestrings)
    println("******************************** " + "linestrings" + " and " + "polygons" + " spatial join ********************************")
    spatialJoinFun(sc, linestrings, polygons)
    println("******************************** " + "linestrings" + " and " + "rectangles" + " spatial join ********************************")
    spatialJoinFun(sc, linestrings, rectangles)
    println("******************************** " + "rectangles" + " and " + "rectangles" + " spatial join ********************************")
    spatialJoinFun(sc, rectangles, rectangles)
    println("******************************** " + "rectangles" + " and " + "polygons" + " spatial join ********************************")
    spatialJoinFun(sc, rectangles, polygons)
    println("******************************** " + "polygons" + " and " + "polygons" + " spatial join ********************************")
    spatialJoinFun(sc, polygons, polygons)

  }

  def spatialJoinFun(sc: SparkContext, leftPath: String, rightPath: String): Unit = {

    var t0 = 0L
    var t1 = 0L
    var count1 = 0L
    //    var count = 0L

    t0 = System.nanoTime()

    val spatialjoinrdd = new SpatialJoinRDD()

    val (leftBufferRDD, rightIndexRDD) = spatialjoinrdd.dataPrepare(sc, leftPath, rightPath)
    leftBufferRDD.persist(StorageLevel.MEMORY_ONLY)
    rightIndexRDD.persist(StorageLevel.MEMORY_ONLY)
        leftBufferRDD.count()
        rightIndexRDD.count()
    t1 = System.nanoTime()

    val partition_index = ((t1 - t0) / 1E9)

    println("Partition and index Time: " + partition_index + " sec")
    t0 = 0
    t1 = 0


    t0 = System.nanoTime()
    val result = spatialjoinrdd.spatialJoint(leftBufferRDD, rightIndexRDD).count()

    t1 = System.nanoTime()
    val Join_time = ((t1 - t0) / 1E9)
    println("Join Time: " + Join_time + " sec")
    val total_time = Join_time+partition_index
    println("Total Time: " + total_time + " sec")

    leftBufferRDD.unpersist()
    rightIndexRDD.unpersist()
  }
}