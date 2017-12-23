package com.esri.dbscan

import java.io.{File, PrintWriter}
import java.util.UUID

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

case class ClusterPoint(pointId: Int, x: Double, y: Double, clusterId: String) {
  def toText(): String = {
    s"$pointId $x $y"
  }
}

object ClusterPoint {
  def apply(line: String): ClusterPoint = {
    line.split(',') match {
      case Array(id, x, y, c) => new ClusterPoint(id.toInt, x.toDouble, y.toDouble, c)
    }
  }
}

class DBSCANSpec extends FlatSpec with Matchers {

  it should "test Erik's use case" in {
    val output = UUID.randomUUID().toString
    val outputPath = s"target/$output"
    val conf = new SparkConf()
    conf.setAppName("DBSCANSpec")
    conf.setMaster("local[2]")
    conf.set("spark.ui.enabled", "false")
    conf.set(DBSCANProp.INPUT_PATH, "src/test/resources/erik.txt")
    conf.set(DBSCANProp.OUTPUT_PATH, outputPath)
    conf.set(DBSCANProp.DBSCAN_EPS, "2")
    conf.set(DBSCANProp.DBSCAN_MIN_POINTS, "3")
    conf.set(DBSCANProp.DBSCAN_NUM_PARTITIONS, "2")
    val sc = new SparkContext(conf)
    try {
      DBSCANApp.doMain(sc, sc.getConf)
    } finally {
      sc.stop()
    }
    val part0 = new File(outputPath, "part-00000")
    val part1 = new File(outputPath, "part-00001")
    part0.exists shouldBe true
    part1.exists shouldBe true
    val lines0 = Source.fromFile(part0).getLines.toSeq
    val lines1 = Source.fromFile(part1).getLines.toSeq
    val points = (lines0 ++ lines1).map(ClusterPoint(_))
    points should contain theSameElementsAs Seq(
      ClusterPoint("0,37.6,30.0,1"),
      ClusterPoint("1,39.2,30.0,1"),
      ClusterPoint("2,40.8,30.0,1"),
      ClusterPoint("3,42.4,30.0,1")
    )
  }

  it should "test Randall 0 - all points in a cell" in {
    val output = UUID.randomUUID().toString
    val outputPath = s"target/$output"
    val conf = new SparkConf()
    conf.setAppName("DBSCANSpec")
    conf.setMaster("local[2]")
    conf.set("spark.ui.enabled", "false")
    conf.set(DBSCANProp.INPUT_PATH, "src/test/resources/randall_0.txt")
    conf.set(DBSCANProp.OUTPUT_PATH, outputPath)
    conf.set(DBSCANProp.DBSCAN_EPS, "2")
    conf.set(DBSCANProp.DBSCAN_MIN_POINTS, "2")
    conf.set(DBSCANProp.DBSCAN_NUM_PARTITIONS, "2")
    val sc = new SparkContext(conf)
    try {
      DBSCANApp.doMain(sc, sc.getConf)
    } finally {
      sc.stop()
    }
    val part0 = new File(outputPath, "part-00000")
    val part1 = new File(outputPath, "part-00001")
    part0.exists shouldBe true
    part1.exists shouldBe true
    val lines0 = Source.fromFile(part0).getLines.toSeq
    val lines1 = Source.fromFile(part1).getLines.toSeq
    val points = (lines0 ++ lines1).map(ClusterPoint(_))
    points should contain theSameElementsAs Seq(
      ClusterPoint("0,29.5,29.5,1:1:-1"),
      ClusterPoint("1,30.5,29.5,1:1:-1"),
      ClusterPoint("2,30.0,30.5,1:1:-1")
    )
  }

  it should "test Randall 1 - each point is in a cell" in {
    val output = UUID.randomUUID().toString
    val outputPath = s"target/$output"
    val conf = new SparkConf()
    conf.setAppName("DBSCANSpec")
    conf.setMaster("local[2]")
    conf.set("spark.ui.enabled", "false")
    conf.set(DBSCANProp.INPUT_PATH, "src/test/resources/randall_1.txt")
    conf.set(DBSCANProp.OUTPUT_PATH, outputPath)
    conf.set(DBSCANProp.DBSCAN_EPS, "2")
    conf.set(DBSCANProp.DBSCAN_MIN_POINTS, "2")
    conf.set(DBSCANProp.DBSCAN_NUM_PARTITIONS, "2")
    val sc = new SparkContext(conf)
    try {
      DBSCANApp.doMain(sc, sc.getConf)
    } finally {
      sc.stop()
    }
    val part0 = new File(outputPath, "part-00000")
    val part1 = new File(outputPath, "part-00001")
    part0.exists shouldBe true
    part1.exists shouldBe true
    val lines0 = Source.fromFile(part0).getLines.toSeq
    val lines1 = Source.fromFile(part1).getLines.toSeq
    val points = (lines0 ++ lines1).map(ClusterPoint(_))
    points should contain theSameElementsAs Seq(
      ClusterPoint("0,19.5,19.5,1"),
      ClusterPoint("1,20.5,19.5,1"),
      ClusterPoint("2,20.5,20.5,1")
    )
  }

  it should "test Randall 2 - all points are in cells" in {
    val output = UUID.randomUUID().toString
    val outputPath = s"target/$output"
    val conf = new SparkConf()
    conf.setAppName("DBSCANSpec")
    conf.setMaster("local[2]")
    conf.set("spark.ui.enabled", "false")
    conf.set(DBSCANProp.INPUT_PATH, "src/test/resources/randall_2.txt")
    conf.set(DBSCANProp.OUTPUT_PATH, outputPath)
    conf.set(DBSCANProp.DBSCAN_EPS, "2")
    conf.set(DBSCANProp.DBSCAN_MIN_POINTS, "2")
    conf.set(DBSCANProp.DBSCAN_NUM_PARTITIONS, "2")
    val sc = new SparkContext(conf)
    try {
      DBSCANApp.doMain(sc, sc.getConf)
    } finally {
      sc.stop()
    }
    val part0 = new File(outputPath, "part-00000")
    val part1 = new File(outputPath, "part-00001")
    part0.exists shouldBe true
    part1.exists shouldBe true
    val lines0 = Source.fromFile(part0).getLines.toSeq
    val lines1 = Source.fromFile(part1).getLines.toSeq
    val points = (lines0 ++ lines1).map(ClusterPoint(_))
    points should contain theSameElementsAs Seq(
      ClusterPoint("0,9.5,9.5,0:0:-1"),
      ClusterPoint("1,10.5,9.5,0:0:-1"),
      ClusterPoint("2,10.5,10.5,0:0:-1"),
      ClusterPoint("3,29.5,29.5,1:1:-1"),
      ClusterPoint("4,30.5,29.5,1:1:-1"),
      ClusterPoint("5,30.5,30.5,1:1:-1")
    )
  }

  it should "test Randall 3 - some points in cell and some on border" in {
    val output = UUID.randomUUID().toString
    val outputPath = s"target/$output"
    val conf = new SparkConf()
    conf.setAppName("DBSCANSpec")
    conf.setMaster("local[2]")
    conf.set("spark.ui.enabled", "false")
    conf.set(DBSCANProp.INPUT_PATH, "src/test/resources/randall_3.txt")
    conf.set(DBSCANProp.OUTPUT_PATH, outputPath)
    conf.set(DBSCANProp.DBSCAN_EPS, "2")
    conf.set(DBSCANProp.DBSCAN_MIN_POINTS, "2")
    conf.set(DBSCANProp.DBSCAN_NUM_PARTITIONS, "2")
    val sc = new SparkContext(conf)
    try {
      DBSCANApp.doMain(sc, sc.getConf)
    } finally {
      sc.stop()
    }
    val part0 = new File(outputPath, "part-00000")
    val part1 = new File(outputPath, "part-00001")
    part0.exists shouldBe true
    part1.exists shouldBe true
    val lines0 = Source.fromFile(part0).getLines.toSeq
    val lines1 = Source.fromFile(part1).getLines.toSeq
    val points = (lines0 ++ lines1).map(ClusterPoint(_))
    points should contain theSameElementsAs Seq(
      ClusterPoint("0,9.5,9.5,0:0:-1"),
      ClusterPoint("1,10.5,9.5,0:0:-1"),
      ClusterPoint("2,10.5,10.5,0:0:-1"),
      ClusterPoint("3,39.5,39.5,1"),
      ClusterPoint("4,40.5,39.5,1"),
      ClusterPoint("5,40.5,40.5,1")
    )
  }

  it should "test diagonal points" in {
    val inpPoints = (9.5 to 30.5 by 1.0).zipWithIndex.map {
      case (d, i) => {
        ClusterPoint(i, d, d, "1")
      }
    }
    val uuid = UUID.randomUUID().toString
    val inputPath = s"target/$uuid.txt"
    val pw = new PrintWriter(inputPath)
    try {
      inpPoints.foreach(point => pw.println(point.toText))
    } finally {
      pw.close()
    }

    val outputPath = s"target/$uuid"
    val conf = new SparkConf()
    conf.setAppName("DBSCANSpec")
    conf.setMaster("local[2]")
    conf.set("spark.ui.enabled", "false")
    conf.set(DBSCANProp.INPUT_PATH, inputPath)
    conf.set(DBSCANProp.OUTPUT_PATH, outputPath)
    conf.set(DBSCANProp.DBSCAN_EPS, "2")
    conf.set(DBSCANProp.DBSCAN_MIN_POINTS, "2")
    conf.set(DBSCANProp.DBSCAN_NUM_PARTITIONS, "2")
    val sc = new SparkContext(conf)
    try {
      DBSCANApp.doMain(sc, sc.getConf)
    } finally {
      sc.stop()
    }
    val part0 = new File(outputPath, "part-00000")
    val part1 = new File(outputPath, "part-00001")
    part0.exists shouldBe true
    part1.exists shouldBe true
    val lines0 = Source.fromFile(part0).getLines.toSeq
    val lines1 = Source.fromFile(part1).getLines.toSeq
    val outPoints = (lines0 ++ lines1).map(ClusterPoint(_))
    outPoints should contain theSameElementsAs inpPoints
  }
}
