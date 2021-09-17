package edu.nefu.DebugQTC.utils

import java.util
import java.util.{ArrayList, List}

import edu.nefu.emus.{FileDataSplitter, GeometryType}
import edu.nefu.format.FormatMapper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Geometry

class SimpleLoader extends Serializable with Loader {

  override def load(sc: SparkContext, path: String, splitter: FileDataSplitter): RDD[Geometry] = {

    this.load(sc, path, splitter, GeometryType.DEAFAULT)
  }

  override def load(sc: SparkContext, path: String, splitter: FileDataSplitter, geometryType: GeometryType): RDD[Geometry] = {
    val rawtext = sc.textFile(path,256)
    val geometryRDD = rawtext.map {
      x =>
        val geometry = new FormatMapper[Geometry](splitter, false).readGeometry(x)
        //                try {
        //                  val wktReader = new WKTReader()
        //                  geometry = wktReader.read(x)
        //                }
        //                catch {
        //                  case e: Exception =>
        //                    val logger = Logger.getLogger(classOf[SimpleLoader])
        //                    logger.error("[GeoSpark] " + e.getMessage)
        //                }
        geometry
    }

    geometryRDD
//    val finallRDD = geometryRDD.filter {
//      x =>
//        (x != null)
//    }
//    finallRDD
  }
}

object SimpleLoader {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("Simple data load")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val loader = new SimpleLoader
    val rdd1 = loader.load(sc, "/home/runxuan/data/Spatial_Compare/points_10M_wkt.csv", FileDataSplitter.WKT).map {
      x =>
        x.getCoordinates.toList.toString
    }
    //    rdd1.foreach(println)
    val count1 = rdd1.count()
    val count2 = loader.load(sc, "/home/runxuan/data/Spatial_Compare/1points_10M_wkt.csv", FileDataSplitter.WKT).count
    println(count1)
    println(count2)
    sc.stop()
  }
}
