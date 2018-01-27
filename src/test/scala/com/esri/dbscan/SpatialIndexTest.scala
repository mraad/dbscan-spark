package com.esri.dbscan

import org.scalatest.{FlatSpec, Matchers}

/**
  */
class SpatialIndexTest extends FlatSpec with Matchers {
  it should "find 3 points" in {

    val points = List(
      DBSCANPoint(0, 4, 4),
      DBSCANPoint(1, 5, 5),
      DBSCANPoint(2, 6, 6),
      DBSCANPoint(3, 1, 1),
      DBSCANPoint(4, 9, 9)
    )
    val grid = points.foldLeft(SpatialIndex(2)) {
      _ + _
    }

    val result = grid.findNeighbors(DBSCANPoint(0, 5.5, 5.5))

    result.length shouldBe 3
    result should contain allOf(points(0), points(1), points(2))
  }

  it should "not find points" in {

    val points = List(
      DBSCANPoint(3, 1, 1),
      DBSCANPoint(4, 9, 9)
    )
    val grid = points.foldLeft(SpatialIndex(2)) {
      _ + _
    }

    val result = grid.findNeighbors(DBSCANPoint(0, 5.5, 5.5))

    result shouldBe empty
  }
}
