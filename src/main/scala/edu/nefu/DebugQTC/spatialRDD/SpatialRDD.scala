package edu.nefu.DebugQTC.spatialRDD

import java.util

import edu.nefu.DebugQTC.buffer_partition.{BufferedPartitioner, TagedGeom}
import edu.nefu.DebugQTC.gqtree.{CellPoint, GQT, GeoObject, Obj}
import edu.nefu.DebugQTC.partition.{EqualPartitioning, FlatGridPartitioner, QuadTreePartitioner, SpatialPartitioner}
import edu.nefu.DebugQTC.sample.RDDSampleUtils
import edu.nefu.emus.{GridType, IndexType}
import edu.nefu.quadtree.{QuadtreePartitioning, StandardQuadTree}
import edu.nefu.spatialRddTool.StatCalculator
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.random.SamplingUtils
import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory, Point}
import org.locationtech.jts.index.{SpatialIndex, strtree}
import org.locationtech.jts.index.quadtree.Quadtree
import org.locationtech.jts.index.strtree.{GeometryItemDistance, STRtree}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SpatialRDD(rDD: RDD[Geometry]) extends Serializable {

  var origionRDD = rDD
  val numPartitions = origionRDD.getNumPartitions
  /**
   * The total number of records.
   */
  var approximateTotalCount: Long = -1
  var partitionedRDD: RDD[Geometry] = null
  var quadPartitionRDD: RDD[TagedGeom[Geometry]] = null
  var indexedRDD: RDD[SpatialIndex] = null
  var gqt_indexedRDD: RDD[GQT] = null

  var buffer_gqt_indexedRDD: RDD[(STRtree, GQT)] = null
  var buffer_partitioner: BufferedPartitioner = null
  var partitionGrids: util.Map[Integer, Envelope] = null
  /**
   * The boundary envelope.
   */
  var boundaryEnvelope: Envelope = null

  /**
   * 实际调用的分区函数
   *
   * @param gridType
   * @param paddedBoundary
   * @param numpartition
   */
  def partition(gridType: GridType, paddedBoundary: Envelope, numpartition: Int): Unit = {
    gridType match {
      case GridType.EQUALGRID => {
        this.partitionedRDD = girdPartitioning(paddedBoundary, numpartition)
      }
      case GridType.QUADTREE => {
        this.partitionedRDD = quadtreePartitioning(paddedBoundary, numpartition)
      }
      case GridType.BUFFEREDTREE => {
        this.quadPartitionRDD = buffertreePartitioning(paddedBoundary, numpartition)
      }
    }
  }

  /**
   * 实际调用的分区函数
   *
   * @param gridType
   */
  def partition(gridType: GridType): Unit = {
    gridType match {
      case GridType.EQUALGRID => {
        this.partitionedRDD = girdPartitioning()
      }

      case GridType.QUADTREE => {
        this.partitionedRDD = quadtreePartitioning()
      }
      case GridType.BUFFEREDTREE => {
        this.quadPartitionRDD = buffertreePartitioning()
      }
    }
  }

  /**
   * 连接查询过程中被查询RDD实际调用
   *
   * @param partitioner
   */
  def partition(partitioner: SpatialPartitioner): Unit = {
    partitioner match {
      case flatGridPartitioner: FlatGridPartitioner => {
        this.partitionedRDD = girdPartitioning(flatGridPartitioner)
      }

      case quadTreePartitioner: QuadTreePartitioner => {
        this.partitionedRDD = quadtreePartitioning(quadTreePartitioner)
      }
      case bufferedPartitioner: BufferedPartitioner => {
        this.quadPartitionRDD = buffertreePartitioning(bufferedPartitioner)
      }
    }
  }


  /**
   * 均匀网格个分区
   *
   * @return
   */
  def girdPartitioning(/*paddedBoundary: Envelope*/): RDD[Geometry] = {
    this.analyze()

    val paddedBoundary = new Envelope(
      boundaryEnvelope.getMinX,
      boundaryEnvelope.getMaxX + 0.01,
      boundaryEnvelope.getMinY,
      boundaryEnvelope.getMaxY + 0.01)

    girdPartitioning(paddedBoundary, numPartitions)
  }

  /**
   * 均匀网格个分区
   *
   * @param paddedBoundary
   * @param numpartition
   * @return
   */
  def girdPartitioning(paddedBoundary: Envelope, numpartition: Int): RDD[Geometry] = {
    //    val num = numpartition
    val equalPartitioning = new EqualPartitioning(paddedBoundary, numpartition)
    val grids = equalPartitioning.getGrids
    this.partitionGrids = equalPartitioning.getPartitionGrids
    /*.asScala*/
    val partitioner = new FlatGridPartitioner(grids)
    val transRDD = origionRDD.flatMap {
      itemgeom =>
        val iter_result = partitioner.placeObject(itemgeom)
        iter_result.asScala
    }.partitionBy(partitioner).map(_._2)
    transRDD
  }

  /**
   * 用于连接查询过程在的被查询数据分区
   *
   * @param partitioner
   * @return
   */
  def girdPartitioning(partitioner: FlatGridPartitioner): RDD[Geometry] = {
    val transRDD = origionRDD.flatMap {
      iter =>
        val iter_result = partitioner.placeObject(iter)
        iter_result.asScala
    }.partitionBy(partitioner).map(_._2)
    transRDD
  }

  /**
   * 四叉树分区
   *
   * @return
   */
  def quadtreePartitioning(): RDD[Geometry] = {
    this.analyze()
    val paddedBoundary = new Envelope(boundaryEnvelope.getMinX, boundaryEnvelope.getMaxX + 0.01, boundaryEnvelope.getMinY, boundaryEnvelope.getMaxY + 0.01)
    this.quadtreePartitioning(paddedBoundary, numPartitions)
  }

  /**
   * 四叉树分区
   *
   * @param lenvelope
   * @param numpartition
   * @return
   */
  def quadtreePartitioning(lenvelope: Envelope, numpartition: Int): RDD[Geometry] = {
    val num = numpartition
    val paddedBoundary = new Envelope(lenvelope.getMinX, lenvelope.getMaxX + 0.01, lenvelope.getMinY, lenvelope.getMaxY + 0.01)
    val samples = this.sample()
    val quadtreePartitioning = new QuadtreePartitioning(samples.toList.asJava, paddedBoundary, num)
    this.partitionGrids = quadtreePartitioning.getPartitionTree.getPartitionGrids
    val partitionTree = quadtreePartitioning.getPartitionTree.asInstanceOf[StandardQuadTree[Geometry]]
    //    println(partitionTree)
    val partitioner = new QuadTreePartitioner(partitionTree)
    //    val grids = partitioner.getGrids.toArray()
    //    println(grids.size)
    //    for( i <- grids){
    //      println(i)
    //    }
    val transRDD = origionRDD.flatMap {
      iter =>
        val iter_result = partitioner.placeObject(iter)
        iter_result.asScala
    }.partitionBy(partitioner).map(_._2)
    transRDD
  }

  /**
   * 用于连接查询过程在的被查询数据分区
   *
   * @param partitioner
   * @return
   */
  def quadtreePartitioning(partitioner: QuadTreePartitioner): RDD[Geometry] = {
    val transRDD = origionRDD.flatMap {
      iter =>
        val iter_result = partitioner.placeObject(iter)
        iter_result.asScala
    }.partitionBy(partitioner).map(_._2)
    transRDD
  }


  /**
   * bufferTree分区,将跨分区的
   *
   * @return
   */
  def buffertreePartitioning(): RDD[TagedGeom[Geometry]] = {
    this.analyze()
    val paddedBoundary = new Envelope(boundaryEnvelope.getMinX, boundaryEnvelope.getMaxX + 0.01, boundaryEnvelope.getMinY, boundaryEnvelope.getMaxY + 0.01)
    this.buffertreePartitioning(paddedBoundary, numPartitions)
  }

  /**
   * bufferTree分区,将跨分区的
   *
   * @param lenvelope
   * @param numpartition
   * @return
   */
  def buffertreePartitioning(lenvelope: Envelope, numpartition: Int): RDD[TagedGeom[Geometry]] = {
    val num = numpartition
    val paddedBoundary = new Envelope(lenvelope.getMinX, lenvelope.getMaxX + 0.01, lenvelope.getMinY, lenvelope.getMaxY + 0.01)
    val samples = this.sample()
    val quadtreePartitioning = new QuadtreePartitioning(samples.toList.asJava, paddedBoundary, num)
    val partitionTree = quadtreePartitioning.getPartitionTree.asInstanceOf[StandardQuadTree[Geometry]]
    //    println(partitionTree)
    val partitioner = new BufferedPartitioner(partitionTree)
    this.buffer_partitioner = partitioner
    this.partitionGrids = quadtreePartitioning.getPartitionTree.getPartitionGrids
    /*.asScala*/
    val transRDD = origionRDD.flatMap {
      iter =>
        val iter_result = partitioner.placegeom(iter)
        iter_result.asScala
    }.partitionBy(partitioner).map(_._2)
    transRDD
  }

  /**
   * 用于连接查询过程在的被查询数据分区
   *
   * @param partitioner
   * @return
   */
  def buffertreePartitioning(partitioner: BufferedPartitioner): RDD[TagedGeom[Geometry]] = {
    this.partitionGrids = partitioner.getQuadTree().getPartitionGrids
    val transRDD = origionRDD.flatMap {
      iter =>
        val iter_result = partitioner.placegeom(iter)
        iter_result.asScala
    }.partitionBy(partitioner).map(_._2)
    transRDD
  }


  /**
   * 数据采样
   *
   * @return
   */
  def sample(): Array[Envelope] = {
    this.analyze()
    val sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(numPartitions, this.approximateTotalCount, -1)
    val fraction = SamplingUtils.computeFractionForSampleSize(sampleNumberOfRecords, approximateTotalCount, false)
    //    println(fraction)
    val samples = this.origionRDD.sample(false, fraction).map(_.getEnvelopeInternal).collect()
    samples
  }

  /**
   * analyze()
   * boundaryEnvelope和approximateTotalCount赋值
   * localOp、globalOp
   */
  def localOp(agg: StatCalculator, obj: Geometry): StatCalculator = StatCalculator.add(agg, obj)

  def globalOp(agg: StatCalculator, obj: StatCalculator): StatCalculator = StatCalculator.combine(agg, obj)

  def analyze(): Unit = {
    val agg = origionRDD.aggregate(new StatCalculator)(localOp, globalOp)
    this.boundaryEnvelope = agg.getBoundary
    this.approximateTotalCount = agg.getCount
  }


  /**
   * 对整个RDD使用普通索引结构进行索引
   *
   * @param indexType
   * @param buildIndexOnSpatialPartitionedRDD
   */
  def build_simple_Index(indexType: IndexType, buildIndexOnSpatialPartitionedRDD: Boolean): Unit = {
    if (buildIndexOnSpatialPartitionedRDD == false) { //This index is built on top of unpartitioned SRDD
      this.indexedRDD = this.origionRDD.mapPartitions {
        iter =>
          simple_partition_index(iter, indexType)
      }
    }
    else {
      if (this.partitionedRDD == null) throw new Exception("[SpatialRDD][build_simple_Index] partitionedRDD 为空. 需要首先调用partitiioning函数")
      this.indexedRDD = this.partitionedRDD.mapPartitionsWithIndex {
        (index, iter) =>
          simple_partition_index(iter, indexType)
      }
    }

  }

  /**
   * 对RDD进行gqt索引
   *
   * @param buildIndexOnSpatialPartitionedRDD
   */
  def build_gqt_Index(buildIndexOnSpatialPartitionedRDD: Boolean): Unit = {
    this.analyze()
    if (buildIndexOnSpatialPartitionedRDD == false) { //This index is built on top of unpartitioned SRDD
      this.gqt_indexedRDD = this.origionRDD.mapPartitions {
        iter =>
          val envelope = boundaryEnvelope
          gqt_partition_index(iter, envelope)
      }
    }
    else {
      if (this.partitionedRDD == null) throw new Exception("[SpatialRDD][build_gqt_Index] partitionedRDD 为空. 需要首先调用partitiioning函数")
      this.gqt_indexedRDD = this.partitionedRDD.mapPartitionsWithIndex {
        (index, iter) =>
          val envelope = this.partitionGrids.get(index)
          gqt_partition_index(iter, envelope)
      }
    }
  }

  /**
   * 建立gqt索引
   */
  def buffer_gqt_Index(): Unit = {
    if (this.quadPartitionRDD == null) throw new Exception("[SpatialRDD][build_gqt_Index] quadPartitionRDD 为空. 需要首先调用partitiioning函数")
    this.buffer_gqt_indexedRDD = this.quadPartitionRDD.mapPartitionsWithIndex {
      (index, iter) =>
        val envelope = this.partitionGrids.get(index)
        buffer_gqt_partition_index(iter, envelope)
    }
  }

  def buffer_gqt_partition_index(objectIterator: Iterator[TagedGeom[Geometry]], envelope: Envelope): Iterator[(STRtree, GQT)] = {
    val left_bot_x = envelope.getMinX
    val left_bot_y = envelope.getMinY
    val right_top_x = envelope.getMaxX
    val right_top_y = envelope.getMaxY
    val array = mutable.Set[Geometry]()
    val gqt = new GQT(1024, 1024, CellPoint(left_bot_x, left_bot_y), CellPoint(right_top_x, right_top_y))
    val strtree = new STRtree
    while (objectIterator.hasNext) {
      val tagedGeom = objectIterator.next()
      if (tagedGeom.multiCopy) {
        val multicopy_geometry = tagedGeom.geometry
        array += tagedGeom.geometry
        strtree.insert(multicopy_geometry.getEnvelopeInternal, multicopy_geometry)
      } else {
        gqt.insertion(tagedGeom.geometry)
      }
    }
    Iterator((strtree, gqt))

  }

  //对每个分区进行索引的函数，build_simple_Index在每个partition中调用该函数
  def simple_partition_index(objectIterator: Iterator[Geometry], indexType: IndexType): Iterator[SpatialIndex] = {

    var spatialIndex: SpatialIndex = null
    //    spatialIndex = if (indexType eq IndexType.RTREE) new STRtree
    //    else new Quadtree
    indexType match {
      case IndexType.RTREE => spatialIndex = new STRtree
      case IndexType.QUADTREE => spatialIndex = new Quadtree
    }

    while (objectIterator.hasNext) {
      val spatialObject = objectIterator.next
      spatialIndex.insert(spatialObject.getEnvelopeInternal, spatialObject)
    }
    spatialIndex.query(new Envelope(0.0, 0.0, 0.0, 0.0))


    Iterator(spatialIndex)

  }

  //对每个分区进行索引的函数，build_gqt_Index在每个partition中调用该函数
  /**
   * @param objectIterator
   * @param envelope
   * @return
   */
  def gqt_partition_index(objectIterator: Iterator[Geometry], envelope: Envelope): Iterator[GQT] = {

    val left_bot_x = envelope.getMinX
    val left_bot_y = envelope.getMinY
    val right_top_x = envelope.getMaxX
    val right_top_y = envelope.getMaxY

    val gqt = new GQT(1024, 1024, CellPoint(left_bot_x, left_bot_y), CellPoint(right_top_x, right_top_y))
    while (objectIterator.hasNext) {
      gqt.insertion(objectIterator.next())
    }
    Iterator(gqt)
  }

  /** *******************************************************************查询算子 *************************************************************************/
  /**
   * 范围查询
   */
  def rangeQuery(queryWindow: Envelope, indexType: IndexType): RDD[Geometry] = {
    val geometryFactory: GeometryFactory = new GeometryFactory()
    val query_geometry = geometryFactory.toGeometry(queryWindow)
    indexType match {
      case null =>
        this.origionRDD.filter {
          spatialObjexts =>
            spatialObjexts.intersects(query_geometry)
        }
      case IndexType.QUADTREE =>
        this.indexedRDD.mapPartitions {
          iter =>
            val quadtree = iter.next().asInstanceOf[Quadtree]
            val list: Iterator[Geometry] = quadtree.query(queryWindow).asInstanceOf[util.List[Geometry]].iterator().asScala
            list
        }
      case IndexType.RTREE =>
        this.indexedRDD.mapPartitions {
          iter =>
            val strtree = iter.next().asInstanceOf[STRtree]
            val list: Iterator[Geometry] = strtree.query(queryWindow).asInstanceOf[util.List[Geometry]].iterator().asScala
            list
        }
      case IndexType.GQT =>
        this.gqt_indexedRDD.mapPartitions {
          iter =>
            val gqt = iter.next()
            var list = gqt.rangQuery(queryWindow).map(_.geometry).iterator
            //             .asJava.iterator().asScala
            list
        }
    }
  }

  /*  def rangeQuery_onbufferpartition(queryWindow: Envelope, indexType: IndexType): RDD[Geometry] = {

      case class Tuple_demo(list: util.List[Geometry], arrayBuffer: ArrayBuffer[Geometry])
      val geometryFactory: GeometryFactory = new GeometryFactory()
      val query_geometry = geometryFactory.toGeometry(queryWindow)
      indexType match {
        case IndexType.GQT =>
          val tmp_result = this.buffer_gqt_indexedRDD.mapPartitions {
            iter =>
              val tmp = iter.next()
              val strtree: STRtree = tmp._1
              val gqt = tmp._2
              val strtree_list = strtree.query(queryWindow).asInstanceOf[util.List[Geometry]]
              var gqt_list = gqt.rangQuery(queryWindow).map(_.geometry)
              Iterator(Tuple_demo(strtree_list, gqt_list))

          }
          val gqt_RDD = tmp_result.map(_.list)
          val str_RDD = tmp_result.map(_.arrayBuffer)
          val result_gqt = gqt_RDD.mapPartitions {
            iter =>
              val list = iter.next()
              list.iterator().asScala
          }
          val result_str = str_RDD.mapPartitions {
            iter =>
              val list = iter.next()
              list.iterator
          }
          result_str.distinct().union(result_gqt)
      }
    }*/

  def rangeQuery_onbufferpartition(queryWindow: Envelope, indexType: IndexType): RDD[Geometry] = {

    case class Tuple_demo(list: util.List[Geometry], arrayBuffer: ArrayBuffer[Geometry])
    val geometryFactory: GeometryFactory = new GeometryFactory()
    val query_geometry = geometryFactory.toGeometry(queryWindow)
    indexType match {
      case IndexType.GQT =>
        val tmp_result = this.buffer_gqt_indexedRDD.mapPartitions {
          iter =>
            var result_list: ArrayBuffer[Geometry] = new ArrayBuffer[Geometry]()
            val partitionID = TaskContext.getPartitionId()
            val extend = partitionGrids.get(partitionID)
            if (!extend.intersects(queryWindow)) {
              result_list.iterator
            }
            else {
              val tmp = iter.next()
              val strtree: STRtree = tmp._1
              val gqt = tmp._2

              result_list = gqt.rangQuery(queryWindow).map(_.geometry)
              val buffer_candidate = strtree.query(queryWindow).toArray()
              for (i <- buffer_candidate) {
                val current_geom = i.asInstanceOf[Geometry]
                val interct_Envelope = current_geom.getEnvelopeInternal.intersection(queryWindow)
                if (extend.covers(new Coordinate(interct_Envelope.getMinX, interct_Envelope.getMinY))) {
                  result_list.append(current_geom)
                }
              }
              result_list.iterator
            }
        }
        tmp_result
    }
  }


  def knnQuery(queryCenter: Point, k: Int, isGQT: Boolean): Array[Geometry] = {

    case class GeoWithDistance(geometry: Geometry, distance: Double)

    def objOrder(d: GeoWithDistance) = d.distance

    if (isGQT) {
      val resultRDD = this.gqt_indexedRDD.mapPartitions {
        iter =>
          val gqt = iter.next()
          var list = gqt.kNN(queryCenter, k).iterator //.asJava.iterator().asScala
          list
      }
      resultRDD.takeOrdered(k)(Ordering[Double].on(x => x.distance)).map(_.geometry)
    } else {
      val resultlist = this.indexedRDD.mapPartitions {
        iter =>
          val strTree = (iter.next()).asInstanceOf[STRtree]
          var list = strTree.nearestNeighbour(queryCenter.getEnvelopeInternal(), queryCenter, new GeometryItemDistance(), k).iterator
          //          Iterator(list)
          list
      }.map {
        x =>
          GeoWithDistance(x.asInstanceOf[Geometry], queryCenter.distance(x.asInstanceOf[Geometry]))
      }.collect
      val obj_set = new mutable.PriorityQueue[GeoWithDistance]()(Ordering.by(objOrder).reverse)
      resultlist.foreach {
        x => obj_set.enqueue(x)
      }
      var result = new Array[Geometry](k)

      for (i <- 0 to k - 1) {
        result(i) = obj_set.dequeue().geometry
      }
      result
    }

  }


  def knnQuery_onbufferpartition(queryCenter: Point, k: Int): Array[Geometry] = {
    case class GeoWithDistance(geometry: Geometry, distance: Double)
    def objOrder(d: GeoObject[Geometry]) = d.distance

    val result_tmp: RDD[mutable.PriorityQueue[GeoObject[Geometry]]] = this.buffer_gqt_indexedRDD.mapPartitions {
      iter =>
        val obj_set = new mutable.PriorityQueue[GeoObject[Geometry]]()(Ordering.by(objOrder))
        val partitionID = TaskContext.getPartitionId()
        val extend = partitionGrids.get(partitionID)
        if (!extend.contains(queryCenter.getCoordinate)) {
          Iterator(obj_set)
        } else {
          val tmp = iter.next()
          val strtree: STRtree = tmp._1
          val gqt = tmp._2
          var gqt_knns = gqt.kNN(queryCenter, k).iterator

          for (i <- gqt_knns) {
            obj_set.enqueue(i)
          }

          if (strtree.size() != 0) {
            var list = strtree.nearestNeighbour(queryCenter.getEnvelopeInternal(), queryCenter, new GeometryItemDistance(), k)

            for (i <- list) {
              val tempdistance = queryCenter.distance(i.asInstanceOf[Geometry])

              val queue_head = obj_set.dequeue()
              if (queue_head.distance > tempdistance) {
                val geoObject_temp = new GeoObject[Geometry](i.asInstanceOf[Geometry])
                geoObject_temp.distance = queryCenter.distance(i.asInstanceOf[Geometry])
                obj_set.enqueue(geoObject_temp)
              } else {
                obj_set.enqueue(queue_head)
              }
            }
          }

          Iterator(obj_set)
        }
    }.filter(!_.isEmpty)
    val temp_queue = result_tmp.collect()(0)
    val queue_head = temp_queue.head
    val tempdistance = queue_head.distance
    val x = queryCenter.getCoordinate.x
    val y = queryCenter.getCoordinate.y
    val queryWindow = new Envelope(x - tempdistance, x + tempdistance, y - tempdistance, y + tempdistance)
    buffer_gqt_indexedRDD.context.broadcast(queryWindow)

    val additionRDD = this.buffer_gqt_indexedRDD.mapPartitions {
      iter =>
        val obj_set = new mutable.PriorityQueue[GeoObject[Geometry]]()(Ordering.by(objOrder))
        val partitionID = TaskContext.getPartitionId()
        val extend = partitionGrids.get(partitionID)
        var result_list: ArrayBuffer[Geometry] = new ArrayBuffer[Geometry]()
        if (extend.contains(queryCenter.getCoordinate)) {
          result_list.iterator
        } else if (extend.intersects(queryWindow)) {

          val tmp = iter.next()
          val strtree: STRtree = tmp._1
          val gqt = tmp._2

          result_list = gqt.rangQuery(queryWindow).map(_.geometry)
          val buffer_candidate = strtree.query(queryWindow).toArray()
          for (i <- buffer_candidate) {
            val current_geom = i.asInstanceOf[Geometry]
            val interct_Envelope = current_geom.getEnvelopeInternal.intersection(queryWindow)
            if (extend.covers(new Coordinate(interct_Envelope.getMinX, interct_Envelope.getMinY))) {
              result_list.append(current_geom)
            }
          }
          result_list.iterator
        } else {
          result_list.iterator
        }
    }
    val additionList = additionRDD.collect()
    for (i <- additionList) {
      val tempdistance = queryCenter.distance(i.asInstanceOf[Geometry])

      val queue_head = temp_queue.dequeue()
      if (queue_head.distance > tempdistance) {
        val geoObject_temp = new GeoObject[Geometry](i.asInstanceOf[Geometry])
        geoObject_temp.distance = queryCenter.distance(i.asInstanceOf[Geometry])
        temp_queue.enqueue(geoObject_temp)
      } else {
        temp_queue.enqueue(queue_head)
      }
    }
    var result = temp_queue.toArray.map(_.geometry)
    result
  }

  def spatialJoin(right: SpatialRDD): RDD[(Geometry, Geometry)] = {
    case class Result(result: util.ArrayList[(Geometry, Geometry)], result_buffer: util.ArrayList[(Geometry, Geometry)])
    val result = this.quadPartitionRDD.zipPartitions(right.buffer_gqt_indexedRDD) {
      (iter_buff_quad_partition, iter_buffgqt) =>

        val partitionID = TaskContext.getPartitionId()
        val extend = partitionGrids.get(partitionID)
        val result = new util.ArrayList[(Geometry, Geometry)]

        if (iter_buffgqt.hasNext && iter_buff_quad_partition.hasNext) {
          val buff_gqt = iter_buffgqt.next()
          val rtree: STRtree = buff_gqt._1
          val gqt: GQT = buff_gqt._2

          while (iter_buff_quad_partition.hasNext) {
            val query_gem: TagedGeom[Geometry] = iter_buff_quad_partition.next()
            val query_envelope = query_gem.geometry.getEnvelopeInternal
            val candidates_rtree = rtree.query(query_envelope).asInstanceOf[util.List[Geometry]].asScala
            val candidates_gqt = gqt.rangQuery(query_envelope).map(_.geometry)

            if (query_gem.multiCopy) {

              for (candidate <- candidates_rtree) {
                if (query_gem.geometry.isInstanceOf[Point]) {
                  result.add((query_gem.geometry, candidate))
                } else {
                  val candidateEnvelope = candidate.getEnvelopeInternal
                  val intersectionEnvelope = candidateEnvelope.intersection(query_envelope)
                  if (extend.covers(new Coordinate(intersectionEnvelope.getMinX, intersectionEnvelope.getMinY))) {
                    if (candidate.intersects(query_gem.geometry)) {
                      result.add((query_gem.geometry, candidate))
                    }
                  }
                }
              }

              for (candidate <- candidates_gqt) {
                if (query_gem.geometry.isInstanceOf[Point]) {
                  result.add((query_gem.geometry, candidate))
                } else {
                  if (candidate.intersects(query_gem.geometry)) {
                    result.add((query_gem.geometry, candidate))
                  }
                }
              }

            } else {
              for (candidate <- candidates_rtree) {
                if (query_gem.geometry.isInstanceOf[Point]) {
                  result.add((query_gem.geometry, candidate))
                } else {
                  if (candidate.intersects(query_gem.geometry)) {
                    result.add((query_gem.geometry, candidate))
                  }
                }
              }
              for (candidate <- candidates_gqt) {

                if (query_gem.geometry.isInstanceOf[Point]) {
                  result.add((query_gem.geometry, candidate))
                } else {
                  if (candidate.intersects(query_gem.geometry)) {
                    result.add((query_gem.geometry, candidate))
                  }
                }
              }
            }
          }
        }
        result.iterator().asScala
    }
    result
  }


}











