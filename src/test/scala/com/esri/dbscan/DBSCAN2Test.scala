package com.esri.dbscan

import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

class DBSCAN2Test extends FlatSpec with Matchers {

  it should "find one cluster" in {
    val points = Array(
      DBSCANPoint(0, 0, 0),
      DBSCANPoint(1, 0, 2),
      DBSCANPoint(2, 0, 4),
      DBSCANPoint(3, 0, 6),
      DBSCANPoint(4, 0, 8),
      DBSCANPoint(5, 3, 0)
    )
    DBSCAN2(2.5, 2)
      .cluster(points)
      .filter(_.clusterID != 0) should contain only(points(0), points(1), points(2), points(3), points(4))
  }

  it should "have 6 clusters and 20 outliers" in {

    val points = Source.fromURL(getClass.getResource("/dat_4_6_6_20.txt")).getLines().map(line => {
      val splits = line.split(' ')
      DBSCANPoint(splits(0).toInt, splits(1).toDouble, splits(2).toDouble)
    }).toIterable

    val results = Source.fromURL(getClass.getResource("/res_4_6_6_20.txt")).getLines().map(line => {
      val splits = line.split(',')
      splits.tail.map(_.toInt)
    }).toArray

    val clusters = DBSCAN2(4, 6)
      .cluster(points)
      .groupBy(_.clusterID)

    clusters.size shouldBe 7
    clusters
      .filterKeys(_ < 0)
      .foreach {
        case (clusterID, iter) => {
          val ids = iter.map(_.id)
          val index = -(clusterID + 1)
          results(index) should contain theSameElementsAs (ids)
        }
      }
  }

  it should "have 20 clusters and 20 outliers" in {

    val points = Source.fromURL(getClass.getResource("/dat_4_10_20_20.txt")).getLines().map(line => {
      val splits = line.split(' ')
      DBSCANPoint(splits(0).toInt, splits(1).toDouble, splits(2).toDouble)
    }).toIterable

    val results = Source.fromURL(getClass.getResource("/res_4_10_20_20.txt")).getLines().map(line => {
      val splits = line.split(',')
      splits.tail.map(_.toInt)
    }).toArray

    val clusters = DBSCAN2(4, 10)
      .cluster(points)
      .groupBy(_.clusterID)

    clusters.size shouldBe 21
    clusters
      .filterKeys(_ < 0)
      .foreach {
        case (clusterID, iter) => {
          val ids = iter.map(_.id)
          val index = -(clusterID + 1)
          results(index) should contain theSameElementsAs (ids)
        }
      }
  }
  
}
