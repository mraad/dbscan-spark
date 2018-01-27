package com.esri.dbscan

import com.esri.dbscan.DBSCANStatus.DBSCANStatus

import scala.collection.mutable

/**
  * A DBSCAN implementation.
  *
  * @param eps       the neighborhood distance.
  * @param minPoints the minimum number of points in a neighborhood to start forming a cluster.
  */
class DBSCAN2(eps: Double, minPoints: Int) extends Serializable {

  private val statusMap = mutable.Map[DBSCANPoint, DBSCANStatus]()

  /**
    * Cluster supplied points.
    *
    * @param points the points to cluster.
    * @return iterable of the points where each point has now a defined clusterID value.
    */
  def cluster(points: Iterable[DBSCANPoint]): Iterable[DBSCANPoint] = {

    val spatialIndex = points.foldLeft(SpatialIndex(eps))(_ + _)
    var clusterID = -1

    points.foreach(point => {
      if (!statusMap.contains(point)) {
        val neighbors = spatialIndex findNeighbors point
        if (neighbors.length < minPoints) {
          statusMap(point) = DBSCANStatus.NOISE
        } else {
          clusterID = expand(point, neighbors, spatialIndex, clusterID)
        }
      }
      /*
      statusMap.get(point) match {
        case None => {
          val neighbors = spatialIndex findNeighbors point
          if (neighbors.length < minPoints) {
            statusMap(point) = DBSCANStatus.NOISE
          } else {
            clusterID = expand(point, neighbors, spatialIndex, clusterID)
          }
        }
        case _ => // Do Nothing, point has a status.
      }
      */
    })

    points
  }

  private def expand(point: DBSCANPoint,
                     neighbors: Seq[DBSCANPoint],
                     spatialIndex: SpatialIndex,
                     clusterID: Int
                    ): Int = {
    point.clusterID = clusterID
    statusMap(point) = DBSCANStatus.CLASSIFIED
    val queue = new mutable.Queue[DBSCANPoint]
    queue ++= neighbors
    while (queue.nonEmpty) {
      val neighbor = queue.dequeue
      statusMap.getOrElse(neighbor, DBSCANStatus.UNCLASSIFIED) match {
        case DBSCANStatus.UNCLASSIFIED => {
          neighbor.clusterID = clusterID
          statusMap(neighbor) = DBSCANStatus.CLASSIFIED
          val neighborNeighbors = spatialIndex findNeighbors neighbor
          if (neighborNeighbors.length >= minPoints) {
            queue ++= neighborNeighbors
          }
        }
        case DBSCANStatus.NOISE => {
          neighbor.clusterID = clusterID
          statusMap(neighbor) = DBSCANStatus.CLASSIFIED
        }
        case _ => // Do Nothing on default, as it was already classified.
      }
    }
    clusterID - 1
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
    * @param minPoints the minimum number of points in a neighborhood to start forming a cluster.
    * @return a DBSCAN instance.
    */
  def apply(eps: Double, minPoints: Int): DBSCAN2 = {
    new DBSCAN2(eps, minPoints)
  }
}
