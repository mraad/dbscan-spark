package com.esri.dbscan

import org.scalatest.{FlatSpec, Matchers}

/**
  * @todo implement without println !!
  */
class GraphTest extends FlatSpec with Matchers {

  ignore should "visit nodes" in {
    val gs = new Graph[Int](Map(
      0 -> Set(1, 2, 3, 4),
      1 -> Set(0),
      2 -> Set(0),
      3 -> Set(0),
      4 -> Set(0),
      10 -> Set(11),
      11 -> Set(10)
    ))
    gs.traverse(0).foreach(println)
  }

  ignore should "construct" in {
    Graph[Int]()
      .addTwoWay(0, 1)
      .addTwoWay(0, 2)
      .addTwoWay(0, 3)
      .addTwoWay(0, 4)
      .addTwoWay(0, 4)
      .traverse(0)
      .foreach(println)
  }

  ignore should "sum together" in {
    val a = Graph[Int]().addTwoWay(0, 1)
    val b = Graph[Int]().addTwoWay(0, 2)
    val c = a + b
    c.traverse(0).foreach(println)
  }


  ignore should "create global ID" in {
    val g = Graph[Int]()
      .addTwoWay(1, 2)
      .addTwoWay(2, 3)
      .addTwoWay(1, 7)
      .addTwoWay(4, 5)
      .addTwoWay(5, 6)

    val m = g.assignGlobalID()

    // println(m)
  }
}
