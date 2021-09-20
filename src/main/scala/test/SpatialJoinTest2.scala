package test

import edu.nefu.DebugQTC.spatialRDD.SpatialRDD
import edu.nefu.DebugQTC.utils.SimpleLoader
import edu.nefu.emus.{FileDataSplitter, GridType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SpatialJoinTest2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GQT-SpatialJoin")
      .setMaster("local[*]")

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //    val points = "/home/runxuan/data/Spatial_Compare/World/single_machine/points_8M_wkt.csv"
    //    val linestrings = "/home/runxuan/data/Spatial_Compare/World/single_machine/linestrings_1M.csv"
    //    val polygons = "/home/runxuan/data/Spatial_Compare/World/single_machine/buildings_1M.csv"
    //    spatialJoinFun(sc, points, points)

    val points = "/home/runxuan/data/Spatial_Compare/World/single_machine/points_2M_wkt.csv"
    val polygons = "/home/runxuan/data/Spatial_Compare/World/single_machine/buildings_2M.csv"
    val rectangles = "/home/runxuan/data/Spatial_Compare/World/single_machine/rectangles_2M.csv"
    val linestrings = "/home/runxuan/data/Spatial_Compare/World/single_machine/linestrings_2M.csv"
//    val points = "hdfs://master:9000/data/World/points_40M_wkt.csv"
//    val rectangles = "hdfs://master:9000/data/World/rectangles_40M.csv"
//    val polygons = "hdfs://master:9000/data/World/buildings_40M.csv"
//    val linestrings = "hdfs://master:9000/data/World/linestrings_40M.csv"
//    println("****************************** " + "points" + " and " + "points" + " spatial join ********************************")
//    spatialJoinFun(sc, points, points)
//    println("******************************** " + "points" + " and " + "rectangles" + " spatial join ********************************")
//    spatialJoinFun(sc, points, rectangles)
//    println("******************************** " + "points" + " and " + "linestrings" + " spatial join ********************************")
//    spatialJoinFun(sc, points, linestrings)
//    println("******************************** " + "points" + " and " + "polygons" + " spatial join ********************************")
//    spatialJoinFun(sc, points, polygons)
//    println("******************************** " + "linestrings" + " and " + "rectangles" + " spatial join ********************************")
//    spatialJoinFun(sc, linestrings, rectangles)
//    println("******************************** " + "linestrings" + " and " + "linestrings" + " spatial join ********************************")
//    spatialJoinFun(sc, linestrings, linestrings)
    println("******************************** " + "linestrings" + " and " + "polygons" + " spatial join ********************************")
    spatialJoinFun(sc, linestrings, polygons)
//    println("******************************** " + "rectangles" + " and " + "rectangles" + " spatial join ********************************")
//    spatialJoinFun(sc, rectangles, rectangles)
//    println("******************************** " + "rectangles" + " and " + "polygons" + " spatial join ********************************")
//    spatialJoinFun(sc, rectangles, polygons)
//    println("******************************** " + "polygons" + " and " + "polygons" + " spatial join ********************************")
//    spatialJoinFun(sc, polygons, polygons)

  }

  def spatialJoinFun(sc: SparkContext, leftPath: String, rightPath: String): Unit = {

    var t0 = 0L
    var t1 = 0L
    var count1 = 0L
    var count = 0L

    val loader = new SimpleLoader

    t0 = System.nanoTime()
    val leftRDD = loader.load(sc, leftPath, FileDataSplitter.WKT)
    val transleftRDD = new SpatialRDD(leftRDD)

    val rightRDD = loader.load(sc, rightPath, FileDataSplitter.WKT)
    val transrightRDD = new SpatialRDD(rightRDD)

    t1 = System.nanoTime()
    val read_time = (t1 - t0) / 1E9


    t0 = System.nanoTime()
    transleftRDD.partition(GridType.BUFFEREDTREE)
    transleftRDD.quadPartitionRDD.persist(StorageLevel.MEMORY_ONLY)
    transleftRDD.quadPartitionRDD.count()
    t1 = System.nanoTime()
    val leftPTime = (t1 - t0) / 1E9

    t0 = System.nanoTime()
    transrightRDD.partition(transleftRDD.buffer_partitioner)
    transrightRDD.buffer_gqt_Index()
    transrightRDD.buffer_gqt_indexedRDD.persist(StorageLevel.MEMORY_ONLY)
    transrightRDD.buffer_gqt_indexedRDD.count()
    t1 = System.nanoTime()
    val rightPTime = (t1 - t0) / 1E9
    val read_partition_time = read_time + leftPTime + rightPTime
    println("Total Reading and Partitioning Time: " + read_partition_time + " sec")

    t0 = System.nanoTime()

    val join_result = transleftRDD.spatialJoin(transrightRDD)
      .count
    t1 = System.nanoTime()
    val join_time = (t1 - t0) / 1E9
    println("Join Time: " + join_time + " sec")
    println("number of results:" + join_result)

    val total_time = read_time + leftPTime + rightPTime + join_time
    println("Total Join Time: " + total_time + " sec")
    println("********************************************************************************************")

    transleftRDD.quadPartitionRDD.unpersist()
    transrightRDD.buffer_gqt_indexedRDD.unpersist()

  }


}
