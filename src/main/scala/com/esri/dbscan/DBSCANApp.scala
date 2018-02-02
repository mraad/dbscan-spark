package com.esri.dbscan

import java.io.{File, FileReader}
import java.util.Properties

import com.esri.dbscan.DBSCANStatus.DBSCANStatus
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

object DBSCANApp extends App {

  type Cluster = (Int, Int, Int) // rowID, colID, clusterID

  def readAppProperties(conf: SparkConf): Unit = {
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
    else {
      Console.err.println(s"The properties file '$filename' does not exist !")
      sys.exit(-1)
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

  readAppProperties(conf)

  val sc = new SparkContext(conf)
  try {
    doMain(sc, sc.getConf)
  } finally {
    sc.stop()
  }

  def dbscan(sc: SparkContext,
             points: RDD[Point],
             eps: Double,
             minPoints: Int,
             cellSize: Double,
             numPartitions: Int
            ): RDD[DBSCANPoint] = {

    val emitted = points
      // Emit each point to all neighboring cell (if applicable)
      .flatMap(point => point.toCells(cellSize, eps).map(_ -> point))
      .groupByKey(numPartitions)
      .flatMap { case (cell, pointIter) => {
        val border = cell.toEnvp(cellSize)
        // Create inner envp
        val inside = border.shrink(eps)
        // Convert the points to dbscan points.
        val points = pointIter
          .map(point => {
            DBSCANPoint(point, cell.row, cell.col, border.isInside(point), inside.toEmitID(point))
          })
        // Perform local DBSCAN on all the points in that cell and identify each local cluster with a negative non-zero value.
        DBSCAN2(eps, minPoints).cluster(points)
      }
      }
      .cache()

    // Create a graph that relates the distributed local clusters based on their common emitted points.
    val graph = emitted
      .filter(_.emitID > 0)
      .map(point => point.id -> (point.row, point.col, point.clusterID))
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
  }

  def doMain(sc: SparkContext, conf: SparkConf): Unit = {
    val inputPath = conf.get(DBSCANProp.INPUT_PATH)
    val outputPath = conf.get(DBSCANProp.OUTPUT_PATH)
    val eps = conf.getDouble(DBSCANProp.DBSCAN_EPS, 5)
    val minPoints = conf.getInt(DBSCANProp.DBSCAN_MIN_POINTS, 5)
    val cellSize = conf.getDouble(DBSCANProp.DBSCAN_CELL_SIZE, eps * 10.0)
    val numPartitions = conf.getInt(DBSCANProp.DBSCAN_NUM_PARTITIONS, 8)

    val fieldSeparator = conf.get(DBSCANProp.FIELD_SEPARATOR, " ") match {
      case "\t" => '\t'
      case text: String => text(0)
    }
    val fieldId = conf.getInt(DBSCANProp.FIELD_ID, 0)
    val fieldX = conf.getInt(DBSCANProp.FIELD_X, 1)
    val fieldY = conf.getInt(DBSCANProp.FIELD_Y, 2)

    val points = sc
      .textFile(inputPath)
      .flatMap(line => {
        // Convert each line to a Point instance.
        try {
          val tokens = line.split(fieldSeparator)
          Some(Point(tokens(fieldId).toLong, tokens(fieldX).toDouble, tokens(fieldY).toDouble))
        } catch {
          case _: Throwable => None
        }
      })
    dbscan(sc, points, eps, minPoints, cellSize, numPartitions)
      .map(_.toText)
      .saveAsTextFile(outputPath)
  }
}
