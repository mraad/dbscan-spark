package com.esri.dbscan

import com.esri.dbscan.DBSCANFlag.DBSCANFlag

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * A DBSCAN implementation.
  *
  * @param eps       the neighborhood distance.
  * @param minPoints the minimum number of points in a neighborhood to start forming a cluster.
  */
class DBSCAN2(eps: Double, minPoints: Int) extends Serializable {

  private val flags = mutable.Map[DBSCANPoint, DBSCANFlag]()

  /**
    * Cluster supplied points.
    *
    * @param points the points to cluster.
    * @return the clusters and the associated supplied points that formed the cluster.
    */
  def cluster(points: Iterable[DBSCANPoint]): Iterable[Iterable[DBSCANPoint]] = {

    val spatialIndex = points.foldLeft(SpatialIndex(eps)) {
      _ + _
    }

    points.flatMap(point => {
      flags.get(point) match {
        case None => {
          val neighbors = spatialIndex findNeighbors point
          if (neighbors.length < minPoints) {
            flags(point) = DBSCANFlag.NOISE
            None
          } else {
            expand(point, neighbors, spatialIndex)
          }
        }
        case _ => None
      }
    })
  }

  private def expand(point: DBSCANPoint,
                     neighbors: Seq[DBSCANPoint],
                     spatialIndex: SpatialIndex
                    ): Option[ArrayBuffer[DBSCANPoint]] = {
    val cluster = new ArrayBuffer[DBSCANPoint]()
    cluster += point
    flags(point) = DBSCANFlag.PART_OF_A_CLUSTER
    val queue = mutable.Queue(neighbors)
    while (queue.nonEmpty) {
      queue
        .dequeue
        .foreach(neighbor => {
          flags.get(neighbor) match {
            case None => {
              cluster += neighbor
              flags(neighbor) = DBSCANFlag.PART_OF_A_CLUSTER
              val neighborNeighbors = spatialIndex findNeighbors neighbor
              if (neighborNeighbors.length >= minPoints) {
                queue += neighborNeighbors
              }
            }
            case Some(DBSCANFlag.NOISE) => {
              cluster += neighbor
              flags(neighbor) = DBSCANFlag.PART_OF_A_CLUSTER
            }
            case _ => // Do Nothing on default
          }
        })
    }
    Some(cluster)
  }
}

/**
  * Companion object.
  */
object DBSCAN2 extends Serializable {
  /**
    * Create a new DBSCAN instance.
    *
    * @param eps       the neighborhood distance.
    * @param minPoints the min number of points in a cluster.
    * @return a DBSCAN instance.
    */
  def apply(eps: Double, minPoints: Int): DBSCAN2 = {
    new DBSCAN2(eps, minPoints)
  }
}
