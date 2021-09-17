package test

import java.io.{File, PrintWriter}

import edu.nefu.DebugQTC.spatialRDD.SpatialRDD
import edu.nefu.DebugQTC.utils.SimpleLoader
import edu.nefu.emus.{FileDataSplitter, GridType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object BufferPartition_Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GQT-SpatialJoin")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    sc.setLogLevel("ERROR")

    //    val polygons = "hdfs://master:9000/data/World/buildings_80M.csv"
    //    val linestrings = "hdfs://master:9000/data/World/linestrings_72M.csv"

    val polygons = "/home/runxuan/data/Spatial_Compare/World/single_machine/buildings_5M.csv"
    val linestrings ="/home/runxuan/data/Spatial_Compare/World/single_machine/linestrings_4M.csv"

//    val writer = new PrintWriter(new File("/home/runxuan/data/Spatial_Compare/World/single_machine/buffer_result.csv"))

    println("polygon")
//    computeBufferratio(sc, polygons, 32)
//    computeBufferratio(sc, polygons, 64)
//    computeBufferratio(sc, polygons, 128)
    computepartition(sc, polygons, 512)
//    computeBufferratio(sc, polygons, 512)
//    computeBufferratio(sc, polygons, 1024)
//    println("linestrings")
//    computeBufferratio(sc, linestrings, 32)
//    computeBufferratio(sc, linestrings, 64)
//    computeBufferratio(sc, linestrings, 128)
//    computeBufferratio(sc, linestrings, 256)
//    computeBufferratio(sc, linestrings, 512)
//    computeBufferratio(sc, linestrings, 1024)
  }

  def computeBufferratio(sc: SparkContext, path: String, partitionnumber: Int): Unit = {
    val loader = new SimpleLoader
    val initrdd = loader.load(sc, path, FileDataSplitter.WKT)
    val data = initrdd.repartition(partitionnumber)
    data.persist(StorageLevel.MEMORY_ONLY)
    val transleftRDD = new SpatialRDD(data)
    transleftRDD.partition(GridType.BUFFEREDTREE)
    val bufferRDD = transleftRDD.quadPartitionRDD.filter(_.multiCopy)
    val buffersize = bufferRDD. /*distinct().*/ count()
    val count = data.count()
    val buffer_ratio = buffersize.toDouble / count
    println("buffer_ratio: " + buffer_ratio)
    data.unpersist()

  }

  def computepartition(sc: SparkContext, path: String, partitionnumber: Int): Unit = {
    val loader = new SimpleLoader
    val initrdd = loader.load(sc, path, FileDataSplitter.WKT)
    val data = initrdd.repartition(partitionnumber)
    data.persist(StorageLevel.MEMORY_ONLY)
    val transleftRDD = new SpatialRDD(data)
    transleftRDD.partition(GridType.BUFFEREDTREE)
    val count = transleftRDD.quadPartitionRDD.count()/(transleftRDD.quadPartitionRDD.getNumPartitions)
    sc.broadcast(count)
    val partitionRDD = transleftRDD.quadPartitionRDD.mapPartitions{
      iter=>
        Iterator(iter.size/count)
    }
    val a = partitionRDD.collect()
    var t= 0
    for(i <-a){
      print(i+"\t")
      t+=1
      if(t%30==0){
        println()
      }
    }

    data.unpersist()

  }
}
