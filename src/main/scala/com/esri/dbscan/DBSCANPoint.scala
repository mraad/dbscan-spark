package com.esri.dbscan

/**
  * Internal point representation to perform clustering on.
  *
  * @param id     the point identifier.
  * @param x      the horizontal 2D placement.
  * @param y      the vertical 2D placement.
  * @param row    the cell row to which that point belongs.
  * @param col    the cell column to which that point belongs.
  * @param inside is the point wholly inside the cell.
  * @param emitID the emit identifier.
  */
class DBSCANPoint(val id: Long,
                  val x: Double,
                  val y: Double,
                  val row: Int,
                  val col: Int,
                  val inside: Boolean,
                  val emitID: Byte
                 ) extends Euclid {

  /**
    * The cluster identifier.
    * If the value is negative then this indicates a local cluster
    */
  var clusterID = 0

  /**
    * @return global cluster identifier - if the clusterID value is negative then it has to merged with the row and col properties to make it globally unique. A positive value is already globally unique.
    */
  def globalID = {
    if (clusterID < 0) s"$row:$col:$clusterID" else clusterID.toString
  }

  /**
    * @return simple text representation.
    */
  def toText() = {
    s"$id,$x,$y,$globalID"
  }

  /**
    * @return text representation of this instance.
    */
  override def toString = s"DBSCANPoint($id,$x,$y,$row,$col,$inside,$emitID,$globalID)"
}

/**
  * Companion object to create DBSCANPoint instance.
  */
object DBSCANPoint extends Serializable {
  def apply(point: Point) = {
    new DBSCANPoint(point.id, point.x, point.y, 0, 0, true, 0)
  }

  def apply(id: Long, x: Double, y: Double) = {
    new DBSCANPoint(id, x, y, 0, 0, true, 0)
  }

  def apply(id: Long, x: Double, y: Double, emitID: Byte) = {
    new DBSCANPoint(id, x, y, 0, 0, true, emitID)
  }

  def apply(point: Point, row: Int, col: Int, inside: Boolean, emitID: Byte) = {
    new DBSCANPoint(point.id, point.x, point.y, row, col, inside, emitID)
  }
}
