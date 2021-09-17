package edu.nefu.DebugQTC.gqtree

class Grid(cell_number_x: Int, cell_number_y: Int, root1: Node) extends Serializable {

  var cell: Array[Array[Node]] = Array.fill(cell_number_x, cell_number_y)(root1)

}
