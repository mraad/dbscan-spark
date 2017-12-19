package com.esri.dbscan

import org.apache.commons.math3.ml.clustering.{DBSCANClusterer, DoublePoint}
import org.scalatest._

import scala.collection.JavaConversions._
import scala.io.Source

class DBSCANTest extends FlatSpec with Matchers {

  it should "cluster" in {
    val points = Array(
      DBSCANPoint(0, 9, 9),
      DBSCANPoint(1, 11, 9)
    )

    val clusters = DBSCAN2(3, 2).cluster(points).toList
    clusters.headOption shouldBe defined
  }

  it should "find one cluster" in {
    val points = Array(
      DBSCANPoint(0, 0, 0),
      DBSCANPoint(1, 0, 2),
      DBSCANPoint(2, 0, 4),
      DBSCANPoint(3, 0, 6),
      DBSCANPoint(4, 0, 8),
      DBSCANPoint(5, 3, 0)
    )
    val clusters = DBSCAN2(2.5, 2).cluster(points)
    clusters.headOption shouldBe defined
    clusters.head should contain only(points(0), points(1), points(2), points(3), points(4))
  }

  /**
    * http://people.cs.nctu.edu.tw/~rsliang/dbscan/testdatagen.html
    */
  it should "have 6 clusters and 20 outliers" in {

    val points = Source.fromURL(getClass.getResource("/dat_4_6_6_20.txt")).getLines().map(line => {
      val splits = line.split(' ')
      DBSCANPoint(splits(0).toInt, splits(1).toDouble, splits(2).toDouble)
    }).toArray

    val results = Source.fromURL(getClass.getResource("/res_4_6_6_20.txt")).getLines().map(line => {
      val splits = line.split(',')
      splits.tail.map(_.toInt)
    }).toArray

    val clusters = DBSCAN2(4, 6).cluster(points).toList

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

  it should "have 20 clusters and 20 outliers" in {

    val points = Source.fromURL(getClass.getResource("/dat_4_10_20_20.txt")).getLines().map(line => {
      val splits = line.split(' ')
      DBSCANPoint(splits(0).toInt, splits(1).toDouble, splits(2).toDouble)
    }).toArray

    val results = Source.fromURL(getClass.getResource("/res_4_10_20_20.txt")).getLines().map(line => {
      val splits = line.split(',')
      splits.tail.map(_.toInt)
    }).toArray

    val clusters = DBSCAN2(4, 10).cluster(points).toList

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

  it should "test Erik's use case" in {
    val points = Source.fromURL(getClass.getResource("/erik.txt")).getLines().map(line => {
      line.split(' ') match {
        case Array(id, x, y) => DBSCANPoint(id.toInt, x.toDouble, y.toDouble)
      }
    }).toIterable
    val clusters = DBSCAN2(2, 3).cluster(points)
    clusters.headOption shouldBe defined
    clusters.head should contain theSameElementsAs points
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
