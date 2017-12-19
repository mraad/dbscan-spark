package com.esri.dbscan

import java.io.{File, FileReader}
import java.util.Properties

import com.esri.dbscan.DBSCANStatus.DBSCANStatus
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

object DBSCANApp extends App {

  type Cluster = (Int, Int, Int) // rowID, colID, clusterID

  def readAppProperties() = {
    val filename = args.length match {
      case 0 => "application.properties"
      case _ => args(0)
    }
    val file = new File(filename)
    if (file.exists()) {
      val reader = new FileReader(file)
      try {
        val properties = new Properties()
        properties.load(reader)
        properties.asScala.foreach { case (k, v) => {
          conf.set(k, v)
        }
        }
      }
      finally {
        reader.close()
      }
    }
  }

  val conf = new SparkConf()
    .setAppName("DBSCANApp")
    .set("spark.app.id", "DBSCANApp")
    .registerKryoClasses(Array(
      classOf[Cell],
      classOf[DBSCAN2],
      classOf[DBSCANStatus],
      classOf[DBSCANPoint],
      classOf[Envp],
      classOf[Graph[Cluster]],
      classOf[Point],
      classOf[SpatialIndex]
    ))

  readAppProperties()

  val sc = new SparkContext(conf)
  try {
    val inputPath = conf.get("input.path")
    val outputPath = conf.get("output.path")
    val eps = conf.getDouble("dbscan.eps", 5)
    val minPoints = conf.getInt("dbscan.min.points", 5)
    val cellSize = conf.getDouble("dbscan.cell.size", eps * 10.0)
    val numPartitions = conf.getInt("dbscan.num.partitions", 8)

    val fieldSeparator = conf.get("field.separator", " ")(0)
    val fieldId = conf.getInt("field.id", 0)
    val fieldX = conf.getInt("field.x", 1)
    val fieldY = conf.getInt("field.y", 2)

    val emitted = sc
      .textFile(inputPath)
      .map(line => {
        // Convert each line to a Point instance.
        val tokens = line.split(fieldSeparator)
        Point(tokens(fieldId).toLong, tokens(fieldX).toDouble, tokens(fieldY).toDouble)
      })
      // Emit each point to all neighboring cell (if applicable)
      .flatMap(point => point.toCells(cellSize, eps).map(_ -> point))
      .groupByKey(numPartitions)
      .flatMap { case (cell, pointIter) => {
        val border = cell.toEnvp(cellSize)
        val inside = border.shrink(eps)
        val points = pointIter.map(point => {
          DBSCANPoint(point, cell.row, cell.col, border.isInside(point), inside.toEmitID(point))
        })
        // Perform local DBSCAN on all the points in that cell and identify each local cluster with a negative non-zero value.
        DBSCAN2(eps, minPoints)
          .cluster(points)
          .zipWithIndex
          .flatMap {
            case (cluster, index) => {
              val clusterID = -1 - index
              cluster.map(point => {
                point.clusterID = clusterID
                point
              })
            }
          }
      }
      }
      .cache()

    // Create a graph that relates the distributed local clusters based on their common emitted points.
    val graph = emitted
      .filter(_.emitID > 0)
      .map(point => point.id -> (point.row, point.col, point.clusterID))
      /*
            .map(point => (point.row, point.col, point.emitID, point.clusterID) -> point.id)
            .reduceByKey(_.min(_))
            .map {
              case ((row, col, _, clusterID), pointID) => pointID ->(row, col, clusterID)
            }
      */
      .groupByKey(numPartitions)
      .aggregate(Graph[Cluster]())(
        (graph, tup) => {
          val orig = tup._2.head
          tup._2.tail.foldLeft(graph) {
            case (g, dest) => g.addTwoWay(orig, dest)
          }
        },
        _ + _
      )

    val globalBC = sc.broadcast(graph.assignGlobalID())

    // Relabel the 'non-noisy' points wholly inside the cell to their global id and write them to the specified output path.
    emitted
      .filter(_.inside)
      .mapPartitions(iter => {
        val globalMap = globalBC.value
        iter.map(point => {
          val key = (point.row, point.col, point.clusterID)
          point.clusterID = globalMap.getOrElse(key, point.clusterID)
          point
        })
      })
      .map(_.toText)
      .saveAsTextFile(outputPath)

  } finally {
    sc.stop()
  }
}
