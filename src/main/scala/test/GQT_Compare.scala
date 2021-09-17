package test

import edu.nefu.DebugQTC.gqtree.{CellPoint, GQT, Global_Val}
import org.apache.lucene.util.RamUsageEstimator
import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory, Point}
import org.locationtech.jts.index.strtree
import org.locationtech.jts.index.strtree.{GeometryItemDistance, STRtree}
import org.locationtech.jts.io.WKTReader

import scala.io.{BufferedSource, Source}

object GQT_Compare {
  def main(args: Array[String]): Unit = {
    val points_10M = "/home/runxuan/data/Spatial_Compare/World/single_machine/points_10M_wkt.csv"
    val points_8M = "/home/runxuan/data/Spatial_Compare/World/single_machine/points_8M_wkt.csv"
    val points_6M = "/home/runxuan/data/Spatial_Compare/World/single_machine/points_6M_wkt.csv"
    val points_4M = "/home/runxuan/data/Spatial_Compare/World/single_machine/points_4M_wkt.csv"
    val points_2M = "/home/runxuan/data/Spatial_Compare/World/single_machine/points_2M_wkt.csv"

    val linestrings_4M = "/home/runxuan/data/Spatial_Compare/World/single_machine/linestrings_4M.csv"
    val linestrings_3M = "/home/runxuan/data/Spatial_Compare/World/single_machine/linestrings_3M.csv"
    val linestrings_2M = "/home/runxuan/data/Spatial_Compare/World/single_machine/linestrings_2M.csv"
    val linestrings_1M = "/home/runxuan/data/Spatial_Compare/World/single_machine/linestrings_1M.csv"


    val buildings_5M = "/home/runxuan/data/Spatial_Compare/World/single_machine/buildings_5M.csv"
    val buildings_4M = "/home/runxuan/data/Spatial_Compare/World/single_machine/buildings_4M.csv"
    val buildings_3M = "/home/runxuan/data/Spatial_Compare/World/single_machine/buildings_3M.csv"
    val buildings_2M = "/home/runxuan/data/Spatial_Compare/World/single_machine/buildings_2M.csv"
    val buildings_1M = "/home/runxuan/data/Spatial_Compare/World/single_machine/buildings_1M.csv"

    testFinal(points_10M)
    testFinal(points_8M)
    testFinal(points_6M)
    testFinal(points_4M)
    testFinal(points_2M)

    testFinal(linestrings_4M)
    testFinal(linestrings_3M)
    testFinal(linestrings_2M)
    testFinal(linestrings_1M)

    testFinal(buildings_5M)
    testFinal(buildings_4M)
    testFinal(buildings_3M)
    testFinal(buildings_2M)
    testFinal(buildings_1M)
  }

  def testFinal(path: String): Unit = {
    //    val gqt = new GQT(1024, 1024, CellPoint(-124.8488253, 24.3965201), CellPoint(-66.8854715, 49.3843548))
    //CellPoint一定要加d，否则被识别为整数
    var gqt = new GQT(1024, 1024, CellPoint(-180.0d, -90.0d), CellPoint(180.0d, 90.0d))
    var rtree = new STRtree()
    val wktReader = new WKTReader
    var t0 = 0L
    var t1 = 0L
    val nQueries = 1
    println("*********************************************************" + path + "**************************************************************")
//    println("********Construct index********")
    var buffer: BufferedSource = Source.fromFile(path)
    //        var buffer: BufferedSource = Source.fromFile("/home/runxuan/data/Spatial_Compare/sample_rectangle.csv")
    val list = buffer.getLines().toList

    //    println(list.size)
    //    println(Global_Val.NODE_MAX_POINT_NUMBER)

    val envelope = new Envelope(-82.7638020000, 42.9526465797, -54.509588996, 38.0106863746)
    t0 = System.nanoTime()
    for (line <- list) {
      val geometry: Geometry = wktReader.read(line)
      gqt.insertion(geometry)
    }
    //materialize
    gqt.rangQuery(envelope).map(_.geometry).size
    t1 = System.nanoTime()
//    println("time for building gqt:" + (t1 - t0) / (1E9) + "s")
    buffer = null
    t0 = System.nanoTime()
    buffer = Source.fromFile(path)
    //        buffer = Source.fromFile("/home/runxuan/data/Spatial_Compare/sample_rectangle.csv")
    for (line <- list) {
      val geometry: Geometry = wktReader.read(line)
      rtree.insert(geometry.getEnvelopeInternal, geometry)
    }
    rtree.query(envelope).size
    t1 = System.nanoTime()
//    println("time for building RTree:" + (t1 - t0) / (1E9) + "s\n")

    println("******Range Query******")
    rangeQuery(envelope, nQueries, gqt, rtree)

    println("******KNN Query******")
    //    val qpoint = wktReader.read("POINT (105.621112 37.063923)").asInstanceOf[Point]
    val random = scala.util.Random
    val geometryFactory = new GeometryFactory()
    var lat = -82.7638020000//(random.nextDouble() * 2 - 1) * 90
    var long = 42//(random.nextDouble() * 2 - 1) * 180
    val qpoint = geometryFactory.createPoint(new Coordinate(long, lat))
    println("******1******")
    knnQuery(qpoint, nQueries, gqt, rtree, 5)
    println("******2******")
    knnQuery(qpoint, nQueries, gqt, rtree, 10)
    println("******3******")
    knnQuery(qpoint, nQueries, gqt, rtree, 20)
    println("******4******")
    knnQuery(qpoint, nQueries, gqt, rtree, 30)
    println("******5******")
    knnQuery(qpoint, nQueries, gqt, rtree, 40)
    println("******6******")
    knnQuery(qpoint, nQueries, gqt, rtree, 50)

    println("**************************************************************")
//    memory_check(gqt, rtree)
    println("*****************************************************************************************************************************")
    rtree = null
    gqt = null
    System.gc()
    System.gc()
  }

  def testFinal2(path: String): Unit = {
    //    val gqt = new GQT(1024, 1024, CellPoint(-124.8488253, 24.3965201), CellPoint(-66.8854715, 49.3843548))
    //CellPoint一定要加d，否则被识别为整数
    var gqt = new GQT(1024, 1024, CellPoint(-180.0d, -90.0d), CellPoint(180.0d, 90.0d))
    val wktReader = new WKTReader
    var t0 = 0L
    var t1 = 0L
    val nQueries = 1
    println("*********************************************************" + path + "**************************************************************")
    println("********Construct index********")
    var buffer: BufferedSource = Source.fromFile(path)
    val list = buffer.getLines().toList

    val envelope = new Envelope(-82.7638020000, 42.9526465797, -54.509588996, 38.0106863746)
    t0 = System.nanoTime()
    for (line <- list) {
      val geometry: Geometry = wktReader.read(line)
      gqt.insertion(geometry)
    }
    //materialize
    gqt.rangQuery(envelope).map(_.geometry).size
    t1 = System.nanoTime()
    println("time for building gqt:" + (t1 - t0) / (1E9) + "s")
    buffer = null
    t0 = System.nanoTime()
    buffer = Source.fromFile(path)

    t1 = System.nanoTime()
    println("time for building RTree:" + (t1 - t0) / (1E9) + "s\n")

    println("******Range Query******")
    gqtrangeQuery(envelope, nQueries, gqt)

    println("******KNN Query******")
    //    val qpoint = wktReader.read("POINT (105.621112 37.063923)").asInstanceOf[Point]
    val random = scala.util.Random
    val geometryFactory = new GeometryFactory()
    var lat = (random.nextDouble() * 2 - 1) * 90
    var long = (random.nextDouble() * 2 - 1) * 180
    val qpoint = geometryFactory.createPoint(new Coordinate(long, lat))
    println("******1******")
    gqtknnQuery(qpoint, nQueries, gqt, 5)
    println("******2******")
    gqtknnQuery(qpoint, nQueries, gqt, 10)
    println("******3******")
    gqtknnQuery(qpoint, nQueries, gqt, 20)
    println("******4******")
    gqtknnQuery(qpoint, nQueries, gqt, 30)
    println("******5******")
    gqtknnQuery(qpoint, nQueries, gqt, 40)
    println("******6******")
    gqtknnQuery(qpoint, nQueries, gqt, 50)

    println("*****************************************************************************************************************************")

    gqt = null
    System.gc()
    System.gc()
  }

  def rangeQuery(envelope: Envelope, nQueries: Int, gqt: GQT, rtree: strtree.STRtree): Unit = {
    var count = 0
    var t0 = 0L
    var t1 = 0L

    count = gqt.rangQuery(envelope).map(_.geometry).size
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count = gqt.rangQuery(envelope).map(_.geometry).size
    }
    t1 = System.nanoTime()
//    println("total number of range query:" + count)
//    println("query time with gqt:" + (t1 - t0) / 1E9)
    println("gqt Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min\n")


    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count = rtree.query(envelope).size
    }
    t1 = System.nanoTime()
//    println("total number of range query:" + count)
//    println("query time with rtree:" + (t1 - t0) / 1E9 + "s")
    println("rtree Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min\n")
  }


  def gqtrangeQuery(envelope: Envelope, nQueries: Int, gqt: GQT): Unit = {
    var count = 0
    var t0 = 0L
    var t1 = 0L

    count = gqt.rangQuery(envelope).map(_.geometry).size
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count = gqt.rangQuery(envelope).map(_.geometry).size
    }
    t1 = System.nanoTime()
    println("total number of range query:" + count)
    println("query time with gqt:" + (t1 - t0) / 1E9)
    println("gqt Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min\n")

  }


  def knnQuery(qpoint: Point, nQueries: Int, gqt: GQT, rtree: strtree.STRtree, k: Int): Unit = {
    var t0 = 0L
    var t1 = 0L

    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      rtree.nearestNeighbour(qpoint.getEnvelopeInternal(), qpoint, new GeometryItemDistance(), k) //.foreach(x=>println(x))
    }
    t1 = System.nanoTime()
//    println("query time with rtree:" + (t1 - t0) / 1E9)
    println("rtree Throughput: \t" + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

    t0 = 0
    t1 = 0

    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      gqt.kNN(qpoint, k) //.foreach(x=> println(x.geometry))
    }
    t1 = System.nanoTime()
//    println("query time with gqt:" + (t1 - t0) / 1E9)
    println("gqt Throughput: \t" + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  }

  def gqtknnQuery(qpoint: Point, nQueries: Int, gqt: GQT, k: Int): Unit = {
    var t0 = 0L
    var t1 = 0L

    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      gqt.kNN(qpoint, k) //.foreach(x=> println(x.geometry))
    }
    t1 = System.nanoTime()
    println("query time with gqt:" + (t1 - t0) / 1E9)
    println("gqt Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min\n")
  }

  def memory_check(gqt: GQT, rtree: strtree.STRtree): Unit = {
    val gqt_size = size(gqt)
    val rtree_size = size(rtree)
    println("gqt内存使用情况*************\n" +
      "计算指定对象本身在堆空间的大小，单位字节:" + gqt_size._1 + "\n" +
      "计算指定对象及其引用树上的所有对象的综合大小:" + gqt_size._2 + "\n")
    println("rtree内存使用情况*************\n" +
      "计算指定对象本身在堆空间的大小，单位字节:" + rtree_size._1 + "\n" +
      "计算指定对象及其引用树上的所有对象的综合大小:" + rtree_size._2 + "\n")
  }

  def size(o: Any): (Long, String) = {
    //计算指定对象本身在堆空间的大小，单位字节
    val shallowSize = RamUsageEstimator.shallowSizeOf(o)
    //计算指定对象及其引用树上的所有对象的综合大小，返回可读的结果，如：2KB
    val humanSize = RamUsageEstimator.humanSizeOf(o)
    Tuple2(shallowSize, humanSize)
  }

  def test(gqt: GQT): Unit = {
    val wktReader = new WKTReader
    val qpoint = wktReader.read("POINT (-84.621112 37.063923)").asInstanceOf[Point]
    for (i <- gqt.findNodes(qpoint)) {
      val list = gqt.findNeighbor(i, qpoint)
      list.foreach(x => println(gqt.caculateDistance(qpoint, x.node)))

      val size = list.size
      println("neighbor:" + size)
    }

  }

}
