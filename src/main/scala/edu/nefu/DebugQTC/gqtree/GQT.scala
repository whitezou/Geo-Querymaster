package edu.nefu.DebugQTC.gqtree

import org.locationtech.jts.geom.{Envelope, Geometry, GeometryFactory, Point}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class GQT /*[T <: Geometry]*/ extends Serializable {
  var grid: Grid = null
  var botLeft, topRight: CellPoint = null
  var root: Node = null
  var grid_width, grid_height, minX, minY, cell_width, cell_height: Double = 0
  var cell_number_x, cell_number_y, node_number: Int = 0

  def this(_cell_number_x: Int, _cell_number_y: Int, _botLeft: CellPoint, _topRight: CellPoint) {
    this()
    this.botLeft = _botLeft
    this.topRight = _topRight
    this.cell_number_x = _cell_number_x
    this.cell_number_y = _cell_number_y
    this.minX = _botLeft.x
    this.minY = _botLeft.y
    this.grid_width = _topRight.x - _botLeft.x
    this.grid_height = _topRight.y - _botLeft.y
    this.cell_width = grid_width / _cell_number_x
    this.cell_height = grid_height / _cell_number_y
    val root_center = new CellPoint((_cell_number_x - 1) / 2, (_cell_number_y - 1) / 2)
    this.root = new Node(root_center, true, false, CellPoint(0, 0), CellPoint(_cell_number_x - 1, _cell_number_y - 1))
    this.grid = new Grid(_cell_number_x, _cell_number_y, root)
  }

  def findNodes(geomtry: Geometry): mutable.Set[Node] = {
    var geoObject = wrap(geomtry)
    val nodeSet = mutable.Set[Node]()
    while (!geoObject.gridQueue.isEmpty) {
      val index_xy = geoObject.gridQueue.dequeue()
      val i: Int = if (index_xy._1 < cell_number_x && index_xy._1 >= 0) index_xy._1 else if (index_xy._1 >= cell_number_x) cell_number_x - 1 else 0
      val j: Int = if (index_xy._2 < cell_number_y && index_xy._2 >= 0) index_xy._2 else if (index_xy._2 >= cell_number_y) cell_number_y - 1 else 0
      try {

        val node = grid.cell(i)(j)
        nodeSet += node
      } catch {
        case e: Exception =>
          println(geomtry)
          println("在gqt中出现一些问题，其中空间对象所在空间网格为i:" + i + "\tj:" + j + ",请忽略或者检查构建过程中输入的Envelope")
      }
    }
    geoObject = null
    nodeSet
  }

  def wrap(geometry: Geometry): GeoObject[Geometry] = {
    val geoObject = new GeoObject(geometry)
    val envelope = geometry.getEnvelopeInternal
    //    println(envelope)
    val bot_left_x = envelope.getMinX
    val bot_left_y = envelope.getMinY
    val top_right_x = envelope.getMaxX
    val top_right_y = envelope.getMaxY

    val bl_x = math.floor((bot_left_x - minX) / cell_width).toInt
    val bl_y = math.floor((bot_left_y - minY) / cell_height).toInt
    val tr_x = math.floor((top_right_x - minX) / cell_width).toInt
    val tr_y = math.floor((top_right_y - minY) / cell_height).toInt
    val geometryFactory = new GeometryFactory
    for (i <- bl_x to tr_x) {
      for (j <- bl_y to tr_y) {
        //grid cell的点坐标
        val cell_bot_l_x = minX + cell_width * i
        val cell_top_r_x = minX + cell_width * (i + 1)
        val cell_bot_l_y = minY + cell_height * j
        val cell_top_r_y = minY + cell_height * (j + 1)
        val cell_envelope = new Envelope(cell_bot_l_x, cell_top_r_x, cell_bot_l_y, cell_top_r_y)

        val envelope_Geometry = geometryFactory.toGeometry(cell_envelope)
        //保存与geometry相交的格网坐标
        if (envelope_Geometry.intersects(geometry) || envelope_Geometry.contains(geometry) || geometry.contains(envelope_Geometry)) {
          geoObject.gridQueue.enqueue((i, j))
        }
      }
    }
    geoObject
  }

  def setNodeNumber(): Unit = {
    var traverse = new mutable.Queue[Node]
    traverse.enqueue(root)
    this.node_number = 0
    while (!traverse.isEmpty) {
      val n: Node = traverse.dequeue()
      if (n.is_leaf) this.node_number += 1
      if (n.se != null) traverse.enqueue(n.se)
      if (n.sw != null) traverse.enqueue(n.sw)
      if (n.ne != null) traverse.enqueue(n.ne)
      if (n.nw != null) traverse.enqueue(n.nw)
    }
  }

  def getNodeEnvelope(n: Node): Envelope = {
    val nodeEnvelope = new Envelope(
      minX + n.boundary_bot_left.IntX * cell_width,
      minX + (n.boundary_top_right.IntX + 1) * cell_width,
      minY + n.boundary_bot_left.IntY * cell_height,
      minY + (n.boundary_top_right.IntY + 1) * cell_height
    )
    nodeEnvelope
  }

  def caculateDistance(geomtry: Geometry, n: Node): Double = {
    var distance: Double = 0
    val nodeEnvelope = getNodeEnvelope(n)
    //    val geometryFactory = new GeometryFactory
    //    val envelope_Geometry = geometryFactory.toGeometry(nodeEnvelope)
    distance = geomtry.getEnvelopeInternal.distance(nodeEnvelope)
    distance
  }

  def initializeVisited(n: Node): Unit = {
    if (n == null) return

    initializeVisited(n.nw)
    initializeVisited(n.ne)
    initializeVisited(n.sw)
    initializeVisited(n.se)
    if (n.visited) n.visited = false
  }

  def updateGrid(n: Node): Unit = {
    if (n.is_leaf) {
      val bot = n.boundary_bot_left
      val top = n.boundary_top_right
      for (i <- bot.IntX to top.IntX) {
        for (j <- bot.IntY to top.IntY) {
          grid.cell(i)(j) = n
        }
      }
      return
    }
    updateGrid(n.nw)
    updateGrid(n.ne)
    updateGrid(n.se)
    updateGrid(n.sw)
  }

  def subdivide(n: Node): Unit = {
    if (n.boundary_bot_left.IntX >= n.boundary_top_right.IntX || n.boundary_bot_left.IntY >= n.boundary_top_right.IntY) {
      n.can_be_subdivide = false
      return
    }
    val q = n.obj_array
    /**
     * 子节点单元格的中心格点坐标
     * 1   2   3   4    5
     * 6   7   8   9    10
     * 11  12  13  14   15
     * 16  17  18  19   20
     * 21  22  23  24   24
     */
    //    println(getNodeEnvelope(n))
    val nw_center = CellPoint((n.center.IntX + n.boundary_bot_left.IntX) / 2, (n.center.IntY + n.boundary_top_right.IntY) / 2) //7
    val ne_center = CellPoint((n.center.IntX + n.boundary_top_right.IntX) / 2, (n.center.IntY + n.boundary_top_right.IntY) / 2) //9
    val se_center = CellPoint((n.center.IntX + n.boundary_top_right.IntX) / 2, (n.center.IntY + n.boundary_bot_left.IntY) / 2) //19
    val sw_center = CellPoint((n.center.IntX + n.boundary_bot_left.IntX) / 2, (n.center.IntY + n.boundary_bot_left.IntY) / 2) //17

    val left_middle_point = CellPoint(n.boundary_bot_left.IntX, n.center.IntY + 1) //11
    val top_middle_point = CellPoint(n.center.IntX, n.boundary_top_right.IntY) //3
    val right_middle_point = CellPoint(n.boundary_top_right.IntX, n.center.IntY) //15
    val bot_middle_point = CellPoint(n.center.IntX + 1, n.boundary_bot_left.IntY) //23
    val right_center_point = CellPoint(n.center.IntX + 1, n.center.IntY + 1)

    n.nw = new Node(nw_center, true, false, left_middle_point, top_middle_point)
    n.ne = new Node(ne_center, true, false, right_center_point, n.boundary_top_right)
    n.se = new Node(se_center, true, false, bot_middle_point, right_middle_point)
    n.sw = new Node(sw_center, true, false, n.boundary_bot_left, n.center)

    n.is_leaf = false
    val geometryFactory = new GeometryFactory
    val nwEnvelope = getNodeEnvelope(n.nw)
    val neEnvelope = getNodeEnvelope(n.ne)
    val seEnvelope = getNodeEnvelope(n.se)
    val swEnvelope = getNodeEnvelope(n.sw)

    while (!q.isEmpty) {
      var loc = q.dequeue()
      val nw_envelope_Geometry = geometryFactory.toGeometry(nwEnvelope)
      val ne_envelope_Geometry = geometryFactory.toGeometry(neEnvelope)
      val se_envelope_Geometry = geometryFactory.toGeometry(seEnvelope)
      val sw_envelope_Geometry = geometryFactory.toGeometry(swEnvelope)

      //左上角
      if (nw_envelope_Geometry.intersects(loc.geometry) || nw_envelope_Geometry.contains(loc.geometry) || loc.geometry.contains(nw_envelope_Geometry)) {
        n.nw.obj_array.enqueue(loc)
        if (n.nw.obj_array.size > Global_Val.NODE_MAX_POINT_NUMBER) subdivide(n.nw)
      }
      //右上角
      else if (ne_envelope_Geometry.intersects(loc.geometry) || ne_envelope_Geometry.contains(loc.geometry) || loc.geometry.contains(ne_envelope_Geometry)) {
        n.ne.obj_array.enqueue(loc)
        if (n.ne.obj_array.size > Global_Val.NODE_MAX_POINT_NUMBER) subdivide(n.ne)
      }
      //右下角
      else if (se_envelope_Geometry.intersects(loc.geometry) || se_envelope_Geometry.contains(loc.geometry) || loc.geometry.contains(se_envelope_Geometry)) {
        n.se.obj_array.enqueue(loc)
        if (n.se.obj_array.size > Global_Val.NODE_MAX_POINT_NUMBER) subdivide(n.se)
      }
      //左下角
      else if (sw_envelope_Geometry.intersects(loc.geometry) || sw_envelope_Geometry.contains(loc.geometry) || loc.geometry.contains(sw_envelope_Geometry)) {
        n.sw.obj_array.enqueue(loc)
        if (n.sw.obj_array.size > Global_Val.NODE_MAX_POINT_NUMBER) subdivide(n.sw)
      }
    }
  }

  def insertion(p: Geometry): Unit = {

    val nodeList = findNodes(p)

    for (n <- nodeList) {
      val geoObject = new GeoObject[Geometry](p)
      geoObject.gridQueue = null
      //检查是否超出阈值，如果是就进行节点分裂
      if ((n.obj_array.size >= Global_Val.NODE_MAX_POINT_NUMBER) && (n.can_be_subdivide == true)) {
        n.obj_array.enqueue(geoObject)
        //        println("正在插入且分裂：" + p+"\t 插前tag 为："+n.can_be_subdivide)
        subdivide(n)
        //        println("正在插入且分裂：" + p+"\t 插后tag 为："+n.can_be_subdivide)
        updateGrid(n)
      } else {
        n.obj_array.enqueue(geoObject)
      }
    }
  }

  //查找当前节点周围的节点，并记录节点到source点的距离
  def findNeighbor(element: Node, source: Point): mutable.Set[Obj[Geometry]] = {
    var n, tmp1, tmp2: Node = null
    val result = mutable.Set[Obj[Geometry]]()
    var p: CellPoint = null
    var Ix, Iy: Int = 0
    var distance: Double = 0
    n = element
    //查找左右的节点
    for (i <- n.boundary_bot_left.IntY to (n.boundary_top_right.IntY)) {
      if (n.boundary_bot_left.IntX > 0) {
        if (grid.cell(n.boundary_bot_left.IntX - 1)(i) != tmp1) {
          tmp1 = grid.cell(n.boundary_bot_left.IntX - 1)(i)
          result += new Obj(tmp1, this.caculateDistance(source, tmp1))
        }
      }
      if (n.boundary_top_right.IntX < (cell_number_x - 1)) {
        if (grid.cell(n.boundary_top_right.IntX + 1)(i) != tmp2) {
          tmp2 = grid.cell(n.boundary_top_right.IntX + 1)(i)
          result += new Obj(tmp2, this.caculateDistance(source, tmp2))
        }
      }
    }
    tmp1 = null
    tmp2 = null
    //查找上下的节点
    for (i <- n.boundary_bot_left.IntX to (n.boundary_top_right.IntX)) {
      if (n.boundary_top_right.IntY < cell_number_y - 1) {
        if (grid.cell(i)(n.boundary_top_right.IntY + 1) != tmp1) {
          tmp1 = grid.cell(i)(n.boundary_top_right.IntY + 1)
          result += new Obj(tmp1, this.caculateDistance(source, tmp1))
        }
      }

      if (n.boundary_bot_left.IntY > 0) {
        if (grid.cell(i)(n.boundary_bot_left.IntY - 1) != tmp2) {
          tmp2 = grid.cell(i)(n.boundary_bot_left.IntY - 1)
          result += new Obj(tmp2, this.caculateDistance(source, tmp2))
        }
      }

    }
    tmp1 = null
    tmp2 = null
    //四个角
    if (n.boundary_bot_left.IntX > 0 && n.boundary_bot_left.IntY > 0) {
      tmp1 = grid.cell(n.boundary_bot_left.IntX - 1)(n.boundary_bot_left.IntY - 1)

      result += new Obj(tmp1, this.caculateDistance(source, tmp1))

    }
    if (n.boundary_bot_left.IntX > 0 && n.boundary_top_right.IntY < cell_number_y - 1) {
      tmp1 = grid.cell(n.boundary_bot_left.IntX - 1)(n.boundary_top_right.IntY + 1)

      result += new Obj(tmp1, this.caculateDistance(source, tmp1))

    }
    if (n.boundary_top_right.IntX < cell_number_x - 1 && n.boundary_top_right.IntY < cell_number_y - 1) {
      tmp1 = grid.cell(n.boundary_top_right.IntX + 1)(n.boundary_top_right.IntY + 1)

      result += new Obj(tmp1, this.caculateDistance(source, tmp1))

    }
    if (n.boundary_top_right.IntX < cell_number_x - 1 && n.boundary_bot_left.IntY > 0) {
      tmp1 = grid.cell(n.boundary_top_right.IntX + 1)(n.boundary_bot_left.IntY - 1)

      result += new Obj(tmp1, this.caculateDistance(source, tmp1))

    }
    result
  }

  def objOrder(d: Obj[Geometry]) = d.distance

  def kNN(source: Point, k: Int): ArrayBuffer[GeoObject[Geometry]] = {
    val source_Envelope = source.getEnvelopeInternal
    var list = new ArrayBuffer[Obj[Geometry]]
//    var  visitedlist = new mutable.HashSet[Obj[Geometry]]()
    val result = new ArrayBuffer[GeoObject[Geometry]]
    val leafnodes = findNodes(source)
    val obj_set = new mutable.PriorityQueue[Obj[Geometry]]()(Ordering.by(objOrder).reverse)
    //    initializeVisited(this.root)

    for (i: Node <- leafnodes) {
      i.visited = false
      val simple_element = new Obj[Geometry](i, caculateDistance(source, i))
      obj_set.enqueue(simple_element)
    }
    var count:Double = 0.0
    while (!obj_set.isEmpty) {
      var element = obj_set.dequeue()
      if (!element.isNode) {
        element.pos.distance = element.distance
        result += (element.pos)
        if (result.size == k) {
          //          return result
          obj_set.clear()
        }
      } else {
//        if(visitedlist.add(element)) {
//          val neighbor = findNeighbor(element.node, source)
//          element.node.visited = true
//          for (single_tmp <- element.node.obj_array) {
//            obj_set.enqueue(new Obj(single_tmp, source_Envelope.distance(single_tmp.geoEnvelope)))
//          }
//          while (!obj_set.isEmpty && element == obj_set.head) obj_set.dequeue()
//          element = null
//          for (i <- neighbor) {
//            obj_set.enqueue(i)
//          }
//        }
        if (!element.node.visited) {
          val neighbor = findNeighbor(element.node, source)
          element.node.visited = true
          list += element
          for (single_tmp <- element.node.obj_array) {
            obj_set.enqueue(new Obj(single_tmp, source_Envelope.distance(single_tmp.geoEnvelope)))
          }
          while (!obj_set.isEmpty && element == obj_set.head) obj_set.dequeue()
          element = null
          for (i <- neighbor) {
            obj_set.enqueue(i)
          }
        }
      }
    }
    list.foreach(_.node.visited = false)
    result
  }

  def rangQuery(envelope: Envelope): ArrayBuffer[GeoObject[Geometry]] = {

    val result = new ArrayBuffer[GeoObject[Geometry]]
    if (new Envelope(this.botLeft.x, this.topRight.x, this.botLeft.y, this.topRight.y).disjoint(envelope)) return result
    val geometryFactory = new GeometryFactory
    val envelope_geometry = geometryFactory.toGeometry(envelope)
    val bot_left_x = envelope.getMinX
    val bot_left_y = envelope.getMinY
    val top_right_x = envelope.getMaxX
    val top_right_y = envelope.getMaxY
    val bl_x_tmp = math.floor((bot_left_x - minX) / cell_width).toInt
    val bl_y_tmp = math.floor((bot_left_y - minY) / cell_height).toInt
    val tr_x_tmp = math.floor((top_right_x - minX) / cell_width).toInt
    val tr_y_tmp = math.floor((top_right_y - minY) / cell_height).toInt

    val bl_x = if (bl_x_tmp < 0) return result else bl_x_tmp
    val bl_y = if (bl_y_tmp < 0) return result else bl_y_tmp
    val tr_x = if (tr_x_tmp >= cell_number_x) return result else tr_x_tmp
    val tr_y = if (tr_y_tmp >= cell_number_y) return result else tr_y_tmp

//    val bl_x = if (bl_x_tmp < 0) 0 else if (bl_x_tmp >= cell_number_x) cell_number_x - 1  else bl_x_tmp
//    val bl_y = if (bl_y_tmp < 0) 0 else if (bl_y_tmp >= cell_number_y) cell_number_y - 1  else bl_y_tmp
//    val tr_x =if (tr_x_tmp < 0) 0 else if (tr_x_tmp >= cell_number_x) cell_number_x - 1 else tr_x_tmp
//    val tr_y = if (tr_y_tmp < 0) 0 else if (tr_y_tmp >= cell_number_y) cell_number_y - 1 else tr_y_tmp
//

    val full_Set = mutable.Set[Node]()
    val edge_Set = mutable.Set[Node]()


    for (i <- bl_x to tr_x) {
      edge_Set += grid.cell(i)(bl_y)
      edge_Set += grid.cell(i)(tr_y)
      for (j <- bl_y to tr_y) {
        full_Set += grid.cell(i)(j)
      }
      for (j <- bl_y to tr_y) {
        edge_Set += grid.cell(bl_x)(j)
        edge_Set += grid.cell(tr_x)(j)
      }
    }
    val inter_Set = full_Set diff edge_Set
    for (i <- inter_Set) {

      result.appendAll(i.obj_array)

    }
    for (i <- edge_Set) {
      val objects = i.obj_array
      for (i <- objects) {
        if (envelope_geometry.intersects(i.geometry) || envelope_geometry.contains(i.geometry) || i.geometry.contains(envelope_geometry)) {
          result += i
        }
      }
    }

    result //.distinct
  }


}
