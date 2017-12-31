package com.esri.dbscan

import com.esri.dbscan.DBSCANStatus.DBSCANStatus

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * A DBSCAN implementation.
  *
  * @param eps       the neighborhood distance.
  * @param minPoints the minimum number of points in a neighborhood to start forming a cluster.
  */
class DBSCAN(eps: Double, minPoints: Int) extends Serializable {

  private val statusMap = mutable.Map[DBSCANPoint, DBSCANStatus]()

  /**
    * Cluster supplied points.
    *
    * @param points the points to cluster.
    * @return the clusters and the associated supplied points that formed the cluster.
    */
  def cluster(points: Iterable[DBSCANPoint]): Iterable[Iterable[DBSCANPoint]] = {

    val spatialIndex = points.foldLeft(SpatialIndex(eps))(_ + _)

    points.flatMap(point => {
      statusMap.get(point) match {
        case None => {
          val neighbors = spatialIndex findNeighbors point
          if (neighbors.length < minPoints) {
            statusMap(point) = DBSCANStatus.NOISE
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
    statusMap(point) = DBSCANStatus.CLASSIFIED
    val queue = new mutable.Queue[DBSCANPoint]
    queue ++= neighbors
    while (queue.nonEmpty) {
      val neighbor = queue.dequeue
      statusMap.get(neighbor) match {
        case None => {
          cluster += neighbor
          statusMap(neighbor) = DBSCANStatus.CLASSIFIED
          val neighborNeighbors = spatialIndex findNeighbors neighbor
          if (neighborNeighbors.length >= minPoints) {
            queue ++= neighborNeighbors
          }
        }
        case Some(DBSCANStatus.NOISE) => {
          cluster += neighbor
          statusMap(neighbor) = DBSCANStatus.CLASSIFIED
        }
        case _ => // Do Nothing on default
      }
    }
    Some(cluster)
  }
}

/**
  * Companion object.
  */
object DBSCAN extends Serializable {
  /**
    * Create a new DBSCAN instance.
    *
    * @param eps       the neighborhood distance.
    * @param minPoints the minimum number of points in a neighborhood to start forming a cluster.
    * @return a DBSCAN instance.
    */
  def apply(eps: Double, minPoints: Int): DBSCAN = {
    new DBSCAN(eps, minPoints)
  }
}
