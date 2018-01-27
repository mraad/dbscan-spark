package com.esri.dbscan

import org.apache.commons.math3.ml.clustering.{DBSCANClusterer, DoublePoint}
import org.scalatest._

import scala.collection.JavaConversions._
import scala.io.Source

@deprecated
class DBSCANTest extends FlatSpec with Matchers {

  ignore should "cluster" in {
    val points = Array(
      DBSCANPoint(0, 9, 9),
      DBSCANPoint(1, 11, 9)
    )

    val clusters = DBSCAN(3, 2).cluster(points).toList
    clusters.headOption shouldBe defined
  }

  ignore should "find one cluster" in {
    val points = Array(
      DBSCANPoint(0, 0, 0),
      DBSCANPoint(1, 0, 2),
      DBSCANPoint(2, 0, 4),
      DBSCANPoint(3, 0, 6),
      DBSCANPoint(4, 0, 8),
      DBSCANPoint(5, 3, 0)
    )
    val clusters = DBSCAN(2.5, 2).cluster(points)
    clusters.headOption shouldBe defined
    clusters.head should contain only(points(0), points(1), points(2), points(3), points(4))
  }

  /**
    * http://people.cs.nctu.edu.tw/~rsliang/dbscan/testdatagen.html
    */
  ignore should "have 6 clusters and 20 outliers" in {

    val points = Source.fromURL(getClass.getResource("/dat_4_6_6_20.txt")).getLines().map(line => {
      val splits = line.split(' ')
      DBSCANPoint(splits(0).toInt, splits(1).toDouble, splits(2).toDouble)
    }).toArray

    val results = Source.fromURL(getClass.getResource("/res_4_6_6_20.txt")).getLines().map(line => {
      val splits = line.split(',')
      splits.tail.map(_.toInt)
    }).toArray

    val clusters = DBSCAN(4, 6).cluster(points).toList

    clusters.length shouldBe 6

    clusters.zipWithIndex.foreach {
      case (cluster, index) => {
        val result = results(index)
        cluster.foreach(point => {
          result should contain(point.id)
        })
        val ids = cluster.map(_.id)
        result.foreach(id => {
          ids should contain(id)
        })
      }
    }
  }

  ignore should "have 20 clusters and 20 outliers" in {

    val points = Source.fromURL(getClass.getResource("/dat_4_10_20_20.txt")).getLines().map(line => {
      val splits = line.split(' ')
      DBSCANPoint(splits(0).toInt, splits(1).toDouble, splits(2).toDouble)
    }).toArray

    val results = Source.fromURL(getClass.getResource("/res_4_10_20_20.txt")).getLines().map(line => {
      val splits = line.split(',')
      splits.tail.map(_.toInt)
    }).toArray

    val clusters = DBSCAN(4, 10).cluster(points).toList

    clusters.length shouldBe 20

    clusters.zipWithIndex.foreach {
      case (cluster, index) => {
        val result = results(index)
        cluster.foreach(point => {
          result should contain(point.id)
        })
        val ids = cluster.map(_.id)
        result.foreach(id => {
          ids should contain(id)
        })
      }
    }
  }

  ignore should "test Erik's use case" in {
    val points = Source
      .fromURL(getClass.getResource("/erik.txt"))
      .getLines()
      .map(DBSCANPoint(_))
      .toIterable
    val clusters = DBSCAN(2, 3).cluster(points)
    clusters.headOption shouldBe defined
    clusters.head should contain theSameElementsAs points
  }

  ignore should "test Randall 0 case, where all points are inside a cell" in {
    val points = Source
      .fromURL(getClass.getResource("/randall_0.txt"))
      .getLines()
      .map(DBSCANPoint(_))
      .toIterable
    val clusters = DBSCAN(2, 2).cluster(points)
    clusters.headOption shouldBe defined
    clusters.head should contain theSameElementsAs Seq(
      DBSCANPoint("0 29.5 29.5"),
      DBSCANPoint("1 30.5 29.5"),
      DBSCANPoint("2 30 30.5")
    )
  }

  ignore should "test Randall 1 case" in {
    val points = Seq(
      DBSCANPoint(0, 37.6, 30.0),
      DBSCANPoint(1, 39.2, 30.0),
      DBSCANPoint(2, 40.8, 30.0)
    )
    val clusters = DBSCAN(2, 3).cluster(points)
    clusters.headOption shouldBe defined
    clusters.head should contain theSameElementsAs Seq(
      DBSCANPoint(0, 37.6, 30.0),
      DBSCANPoint(1, 39.2, 30.0),
      DBSCANPoint(2, 40.8, 30.0)
    )
  }

  ignore should "test Erik's use case using commons math 3" in {
    val points = Source.fromURL(getClass.getResource("/erik.txt")).getLines().map(line => {
      line.split(' ') match {
        case Array(_, x, y) => new DoublePoint(Array(x.toDouble, y.toDouble))
      }
    }).toSeq
    val clusterer = new DBSCANClusterer[DoublePoint](2, 3)
    val list = clusterer.cluster(points)
    list should contain theSameElementsAs points
  }

}
