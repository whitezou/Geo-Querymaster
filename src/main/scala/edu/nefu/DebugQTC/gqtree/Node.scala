package edu.nefu.DebugQTC.gqtree

import org.locationtech.jts.geom.Geometry

import scala.collection.mutable




class CellPoint() extends Serializable {
  var IntX, IntY: Int = 0
  var x, y: Double = 0
  var isVertex: Boolean = false
  var edgeIdx: Int = 0


  def this(_x: Int, _y: Int) {
    this()
    this.IntX = _x
    this.IntY = _y
  }

  def this(_x: Double, _y: Double) {
    this()
    this.x = _x
    this.y = _y
  }

  def caculateDistance(p: CellPoint): Double = {
    math.sqrt(math.pow((this.IntX - p.IntX), 2) +
      math.pow((this.IntY - p.IntY), 2)
    )
  }

}

object CellPoint {
  def apply(_x: Int, _y: Int): CellPoint = new CellPoint(_x, _y)

  def apply(_x: Double, _y: Double): CellPoint = new CellPoint(_x, _y)
}


class Node extends Serializable {
  var sw: Node = null
  var se: Node = null
  var nw: Node = null
  var ne: Node = null
  var can_be_subdivide:Boolean = true
  var is_leaf, visited: Boolean = false

  var obj_array: mutable.Queue[GeoObject[Geometry]] = new mutable.Queue[GeoObject[Geometry]]
  var distance: Double = 0
  var center, boundary_bot_left, boundary_top_right: CellPoint = null

  def this(_center: CellPoint, _is_leaf: Boolean, _visited: Boolean, _boundary_bot_left: CellPoint, _boundary_top_right: CellPoint) {
    this()
    this.center = _center
    this.is_leaf = _is_leaf
    this.visited = _visited
    this.boundary_bot_left = _boundary_bot_left
    this.boundary_top_right = _boundary_top_right

  }
}

class Obj[T <: Geometry] extends Serializable {
  var pos: GeoObject[T] = null
  var node: Node = null
  var distance: Double = 0
  var isNode: Boolean = false

  def this(_pos: GeoObject[T], _distance: Double) {
    this()
    this.pos = _pos
    this.distance = _distance
    this.isNode = false
  }

  def this(_node: Node, _distance: Double) {
    this()
    this.node = _node
    this.distance = _distance
    this.isNode = true
  }
}

