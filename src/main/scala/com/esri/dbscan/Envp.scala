package com.esri.dbscan

/**
  * Spatial envelope represented by the lower left corner and upper right corner.
  */
case class Envp(xmin: Double, ymin: Double, xmax: Double, ymax: Double) {

  /**
    * Convert point to an emit identifier.
    *
    * @param point the point to convert.
    * @return the emit identifier.
    */
  def toEmitID(point: Point): Byte = {
    toEmitID(point.x, point.y)
  }

  /**
    * Convert specified x and y values to an emit identifier.
    * A non-zero emit identifier presents an area around the envelope.
    *
    * 3---4---5
    * |       |
    * |       |
    * 2   0   6
    * |       |
    * |       |
    * 1---8---7
    *
    * @param x the horizontal location.
    * @param y the vertical location.
    * @return an emit identifier.
    */
  def toEmitID(x: Double, y: Double): Byte = {
    if (x < xmin) {
      if (y < ymin) 1 else if (y > ymax) 3 else 2
    } else if (x > xmax) {
      if (y < ymin) 7 else if (y > ymax) 5 else 6
    } else if (y < ymin) {
      8
    } else if (y > ymax) {
      4
    } else {
      0
    }
  }

  /**
    * Shrink the envelope by a specified offset.
    *
    * @param offset the shrink value.
    * @return a new shrunk envelope.
    */
  def shrink(offset: Double): Envp = Envp(xmin + offset, ymin + offset, xmax - offset, ymax - offset)

  /**
    * Check if supplied point is inside the envelope.
    *
    * @param point the point to check.
    * @return true if xmin<= point.x < xmax and ymin<= point.y < ymax otherwise false.
    */
  def isInside(point: Point): Boolean = {
    isInside(point.x, point.y)
  }

  /**
    * Check if supplied x and y values aer inside the envelope.
    *
    * @param x the horizontal location.
    * @param y the vertical location.
    * @return true if xmin <= x < xmax and ymin<= y < ymax otherwise false.
    */
  def isInside(x: Double, y: Double): Boolean = {
    xmin <= x && ymin <= y && x < xmax && y < ymax
  }
}
