package com.esri.dbscan

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Spatial index to quickly location neighbors of a point.
  * The implementation is based on a grid, where all the indexed points are grouped together based on the cell in the grid that they fall into.
  *
  * @param eps the cell size.
  */
case class SpatialIndex(eps: Double) {

  type SIKey = (Int, Int)
  type SIVal = mutable.ArrayBuffer[DBSCANPoint]

  val grid = mutable.Map[SIKey, SIVal]()

  /**
    * Index supplied point.
    *
    * @param point the point to index.
    * @return this spatial index.
    */
  def +(point: DBSCANPoint): SpatialIndex = {
    val c = (point.x / eps).floor.toInt
    val r = (point.y / eps).floor.toInt
    grid.getOrElseUpdate((r, c), ArrayBuffer[DBSCANPoint]()) += point
    this
  }

  /**
    * Find all the neighbors of the specified point.
    * This is a "cheap" implementation, where the neighborhood consists of a bounding box centered on the supplied
    * point, and the width and height of the box are 2 times the spatial index cell size (eps).
    *
    * @param point the point to search around.
    * @return a sequence of points that are in the neighborhood of the supplied point.
    */
  def findNeighbors(point: DBSCANPoint): Seq[DBSCANPoint] = {
    val c = (point.x / eps).floor.toInt
    val r = (point.y / eps).floor.toInt

    val xmin = point.x - eps
    val ymin = point.y - eps
    val xmax = point.x + eps
    val ymax = point.y + eps

    (r - 1 to r + 1).flatMap(i =>
      (c - 1 to c + 1).flatMap(j =>
        grid.getOrElse((i, j), Seq.empty)
          .filter(point => xmin < point.x && point.x < xmax && ymin < point.y && point.y < ymax)
      )
    )
  }
}
