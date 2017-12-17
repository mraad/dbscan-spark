package com.esri.dbscan

import org.scalatest.{FlatSpec, Matchers}

class PointTest extends FlatSpec with Matchers {

  it should "test equal based on id only" in {
    Point(123, 100, 100) shouldBe Point(123, 200, 200)
  }

}
