package test

import java.util

import edu.nefu.DebugQTC.spatialRDD.{SpatialJoinOp, SpatialJoinOp_Geom, SpatialRDD}
import edu.nefu.DebugQTC.utils.SimpleLoader
import edu.nefu.emus.{FileDataSplitter, GridType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Geometry

import scala.collection.JavaConverters._
object SpatialJoinTest3 {
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
    //    var count = 0L

    t0 = System.nanoTime()
    val spatialJoinOp = new SpatialJoinOp_Geom

    val leftGeometryById =spatialJoinOp.loadData(sc, leftPath)

    val rightGeometryById =spatialJoinOp.loadData(sc, rightPath)

    val tuple = spatialJoinOp.partitionwithRBuff(sc,leftGeometryById,rightGeometryById)
    val result = spatialJoinOp.joinQuery(tuple._1,tuple._2).count()

     t1 = System.nanoTime()
    val total_time = ((t1 - t0) / 1E9)

    println("Total Join Time: " + total_time + " sec")
    println("number of results:" + result)



  }


}
