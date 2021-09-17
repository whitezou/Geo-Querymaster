package data_utils

import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.{Envelope, Geometry, GeometryFactory}
import org.locationtech.jts.io.WKTReader

object Sample_data {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val tiger_line = sc.textFile("/home/runxuan/data/Spatial_Compare/World/points_10M.csv")
    val sample_data = tiger_line.sample(true, 0.3)
    val envelope = new Envelope(-125.90, -65.79, 23.18, 49.93)
    val geometryFactory = new GeometryFactory
    val envelope_Geometry = geometryFactory.toGeometry(envelope)
    //envelope_Geometry.
    val sample_filter = sample_data.filter {
      wkt =>
        val wktReader = new WKTReader
        val geometry = wktReader.read(wkt)
        !geometry.disjoint(envelope_Geometry)
    }

    sample_data.coalesce(1, true).saveAsTextFile("/home/runxuan/data/Spatial_Compare/sample_data")
    //    sample_filter.coalesce(1,true).saveAsTextFile("/home/runxuan/data/Spatial_Compare/sample_filter.csv")
    //    val sample_str = sample_data.collect()
    //    println(sample_str)

  }
}
