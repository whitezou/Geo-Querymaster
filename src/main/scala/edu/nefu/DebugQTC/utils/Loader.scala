package edu.nefu.DebugQTC.utils

import edu.nefu.emus.{FileDataSplitter, GeometryType}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

trait Loader {
  def load(sc: SparkContext, path: String, Splitter: FileDataSplitter ): RDD[Geometry]
  def load(sc: SparkContext, path: String, Splitter: FileDataSplitter , geometryType:GeometryType): RDD[Geometry]
}
