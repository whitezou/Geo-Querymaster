package edu.nefu

import edu.nefu.DebugQTC.spatialRDD.SpatialRDD
import edu.nefu.DebugQTC.utils.SimpleLoader
import edu.nefu.emus.{FileDataSplitter, GridType, IndexType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.{Coordinate, Envelope, GeometryFactory, Point}

object TestSpatialRDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("Simple data load")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val loader = new SimpleLoader
        val geomRDD = loader.load(sc, "/home/runxuan/data/Spatial_Compare/points_10M_wkt.csv", FileDataSplitter.WKT)
    //    val geomRDD = loader.load(sc, "/home/runxuan/data/transform_trim/OSM_trans/sports.csv", FileDataSplitter.WKT)
//    val geomRDD = loader.load(sc, "hdfs://219.217.203.1:9000/data/sample_rectangle.csv", FileDataSplitter.WKT)
    //    val paddedBoundary = new Envelope(-180, 180-0.01, -90, 90-0.01)
    val transRDD = new SpatialRDD(geomRDD)

    transRDD.partition(GridType.BUFFEREDTREE)
    transRDD.buffer_gqt_Index()
    println(transRDD.buffer_gqt_indexedRDD.count())
    transRDD.buffer_gqt_indexedRDD.foreach(println)
    /* transRDD.partition(GridType.BUFFEREDTREE /*,paddedBoundary,9*/)
     transRDD.partition(GridType.QUADTREE /*,paddedBoundary,9*/)
     //        transRDD.partitionedRDD.persist(StorageLevel.MEMORY_ONLY)
     transRDD.quadPartitionRDD.persist(StorageLevel.MEMORY_ONLY)
     transRDD.origionRDD.persist(StorageLevel.MEMORY_ONLY)

     //    transRDD.analyze()

     println("分区数====================================================")

     transRDD.quadPartitionRDD.mapPartitions {
       iter =>
         Iterator(iter.size)
     }.collect().foreach(println)

     println("分区数====================================================")


     println("分区数量" + transRDD.quadPartitionRDD.partitions.size)
     //    transRDD.quadPartitionRDD.filter(_.multiCopy == true).foreach {
     //      x =>
     //        println(x.geometry)
     //    }
     println(transRDD.quadPartitionRDD.filter(_.multiCopy == true).count())
     println(transRDD.quadPartitionRDD.count())
     println(transRDD.origionRDD.count())
     println(transRDD.boundaryEnvelope)
     println(transRDD.approximateTotalCount)
     //    transRDD.sample()
     //    transRDD.partitionedRDD.saveAsTextFile("/home/runxuan/data/Spatial_Compare/generated")


     //    transRDD.build_simple_Index(IndexType.QUADTREE, true)
     //    transRDD.build_gqt_Index(true)
     //    transRDD.indexedRDD.foreach(println)
     //    transRDD.gqt_indexedRDD.foreach(println)

     val simple_t0 = System.nanoTime()
     transRDD.build_simple_Index(IndexType.QUADTREE, true)
     transRDD.indexedRDD.count()
     val simple_t1 = System.nanoTime()

     val gqt_t0 = System.nanoTime()
     transRDD.buffer_gqt_Index()
     transRDD.buffer_gqt_indexedRDD.count() //foreach(println)
     val gqt_t1 = System.nanoTime()

     println("四叉树索引："+((simple_t1 - simple_t0)/ 1E9)+ " sec")
     println("gqt索引："+((gqt_t1 - gqt_t0)/1E9)+ " sec")

     transRDD.quadPartitionRDD.mapPartitions {
       iter =>
         var cont_mul: Int = 0
         var cont_sinfle: Int = 0
         while (iter.hasNext) {
           if (iter.next().multiCopy) cont_mul += 1
           else cont_sinfle += 1
         }
         Iterator((cont_mul, cont_sinfle))
     }.foreach {
       x =>
         print("跨分区：" + x._1 + "\t")
         println("单分区：" + x._2)
     }*/
    transRDD.quadPartitionRDD.persist(StorageLevel.MEMORY_ONLY)
    transRDD.buffer_gqt_indexedRDD.persist(StorageLevel.MEMORY_ONLY)
    val geometryFactory = new GeometryFactory()
    //进行范围查询
//    val envelope = new Envelope(-85, -83, 30, 37)
    val envelope = new Envelope(-50.3010141441, -24.9526465797, -53.209588996, -30.1096863746)
    //    println(transRDD.rangeQuery(envelope, IndexType.GQT).count())
    //    println(transRDD.gqt_indexedRDD.count())

    println(transRDD.rangeQuery_onbufferpartition(envelope, IndexType.GQT).count())
    //进行knn查询-GQT
    /*transRDD.build_gqt_Index(false)
    transRDD.knnQuery(geometryFactory.createPoint(new Coordinate(-85, 35)), 5, true).foreach(println(_))*/
    //进行knn查询-R树
    //    transRDD.build_simple_Index(IndexType.RTREE,false)
    //    transRDD.knnQuery(geometryFactory.createPoint(new Coordinate(-85, 35)), 5, false).foreach(println(_))

    sc.stop()
  }
}
