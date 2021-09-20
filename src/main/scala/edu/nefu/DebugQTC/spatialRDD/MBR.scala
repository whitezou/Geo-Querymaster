package edu.nefu.DebugQTC.spatialRDD

import org.locationtech.jts.geom.Envelope


case class MBR(xmin: Double, ymin: Double, xmax: Double, ymax: Double) extends Serializable {

  def union(b: MBR): MBR = {
    new MBR(this.xmin min b.xmin, this.ymin min b.ymin, this.xmax max b.xmax, this.ymax max b.ymax)
  }

  def intersects(b: MBR): Boolean = {
    !(this.xmin > b.xmax || this.xmax < b.xmin || this.ymin > b.ymax || this.ymax < b.ymin)
  }

  def center: (Double, Double) = {
    ((xmin + xmax) / 2, (ymin + ymax) / 2)
  }

  override def toString = {
    xmin + "," + ymin + "," + xmax + "," + ymax
  }

  def toString(separator: String) = {
    xmin + separator + ymin + separator + xmax + separator + ymax
  }

  def toText = {
    s"POLYGON(($xmin $ymin,$xmax $ymin,$xmax $ymax,$xmin $ymax,$xmin $ymin))"
  }

  def toEnvelope: Envelope = {
    new Envelope(xmin, xmax, ymin, ymax)
  }
}
