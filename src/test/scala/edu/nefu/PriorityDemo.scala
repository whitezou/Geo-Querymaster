package edu.nefu

import edu.nefu.DebugQTC.gqtree.Obj
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable
case class Person(name:String, age:Int)

object PriorityDemo {
  def objOrder(person: Person) = person.age
  def main(args: Array[String]): Unit = {
    val obj_set = new mutable.PriorityQueue[Person]()(Ordering.by(objOrder).reverse)
    obj_set.enqueue(Person("ccc",12))
    obj_set.enqueue(Person("bbb",11))
    obj_set.enqueue(Person("aaa",10))
    obj_set.enqueue(Person("ddd",13))

    println(obj_set.dequeue())

  }
}
