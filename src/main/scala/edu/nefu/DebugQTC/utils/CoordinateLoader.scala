package edu.nefu.DebugQTC.utils

import java.util
import java.util.{ArrayList, List}

import edu.nefu.emus.{FileDataSplitter, GeometryType}
import edu.nefu.format.{FormatMapper, LineStringFormatMapper, PointFormatMapper, PolygonFormatMapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

class CoordinateLoader extends Serializable with Loader {

  def load(sc: SparkContext, path: String, splitter: FileDataSplitter, geometryType: GeometryType): RDD[Geometry] = {
    val rawtext = sc.textFile(path)

    var geometryRDD = rawtext.mapPartitions {
      iter =>
        var formatmapper: FormatMapper[Geometry] = new PointFormatMapper(splitter, false).asInstanceOf[FormatMapper[Geometry]]
        geometryType match {
          case GeometryType.POINT =>
            formatmapper = new PointFormatMapper(splitter, false).asInstanceOf[FormatMapper[Geometry]]
          case GeometryType.POLYGON =>
            formatmapper = new PolygonFormatMapper(splitter, false).asInstanceOf[FormatMapper[Geometry]]
          case GeometryType.LINESTRING =>
            formatmapper = new LineStringFormatMapper(splitter, false).asInstanceOf[FormatMapper[Geometry]]
          case GeometryType.RECTANGLE =>
            formatmapper = new PointFormatMapper(splitter, false).asInstanceOf[FormatMapper[Geometry]]
        }
        val result: List[Geometry] = new ArrayList[Geometry]
        while (iter.hasNext) {
          val line = iter.next
          formatmapper.addGeometry(formatmapper.readGeometry(line), result)
        }
        Iterator(result)
    }.flatMap(_.toArray()).map(_.asInstanceOf[Geometry])

//    var geometryRDD = rawtext.map{
//      x=>
//        var formatmapper: FormatMapper[Geometry] = new PointFormatMapper(splitter, false).asInstanceOf[FormatMapper[Geometry]]
//        geometryType match {
//          case GeometryType.POINT =>
//            formatmapper = new PointFormatMapper(splitter, false).asInstanceOf[FormatMapper[Geometry]]
//          case GeometryType.POLYGON =>
//            formatmapper = new PolygonFormatMapper(splitter, false).asInstanceOf[FormatMapper[Geometry]]
//          case GeometryType.LINESTRING =>
//            formatmapper = new LineStringFormatMapper(splitter, false).asInstanceOf[FormatMapper[Geometry]]
//          case GeometryType.RECTANGLE =>
//            formatmapper = new PointFormatMapper(splitter, false).asInstanceOf[FormatMapper[Geometry]]
//        }
//        var geometry =formatmapper.readGeometry(x)
//        geometry
//    }

    val finallRDD = geometryRDD.filter {
      x =>
        (x != null)
    }
    finallRDD
  }

  override def load(sc: SparkContext, path: String, splitter: FileDataSplitter): RDD[Geometry] = {
    this.load(sc, path, splitter, GeometryType.POINT)
  }
}

object CoordinateLoader {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("Simple data load")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val loader = new CoordinateLoader
    val rdd1 = loader.load(sc, "/home/runxuan/data/Spatial_Compare/points_10M.csv", FileDataSplitter.CSV, GeometryType.POINT).map {
      x =>
        x.getCoordinates.toList.toString
    }
    //    rdd1.foreach(println)
    val count1 = rdd1.count()
    //    val count2 = loader.load(sc, "/home/runxuan/data/Spatial_Compare/World/points_10M.csv", FileDataSplitter.WKT).count
    println(count1)
    //    println(count2)
    sc.stop()
  }
}
