package com.esri.dbscan

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * A DBSCAN implementation.
  * This implementation does _not_ return the points that are labeled as 'noise'.
  *
  * @param eps       the neighborhood distance.
  * @param minPoints the minimum number of points in a neighborhood to start forming a cluster.
  */
class DBSCAN(eps: Double, minPoints: Int) extends Serializable {

  private val visited = mutable.Set[DBSCANPoint]()

  /**
    * Cluster supplied points.
    * Note - 'Noise' points are dropped !
    *
    * @param points the points to cluster.
    * @return the clusters and the associated supplied points that formed the cluster.
    */
  def cluster(points: Iterable[DBSCANPoint]): Iterable[Seq[DBSCANPoint]] = {

    val spatialIndex = points.foldLeft(SpatialIndex(eps)) {
      _ + _
    }

    points.flatMap(point => {
      if (!visited.contains(point)) {
        visited += point
        val neighbors = spatialIndex findNeighbors point
        if (neighbors.length < minPoints) {
          None
        } else {
          expand(point, neighbors, spatialIndex)
        }
      }
      else {
        None
      }
    })
  }

  private def expand(point: DBSCANPoint,
                     neighbors: Seq[DBSCANPoint],
                     spatialIndex: SpatialIndex
                    ) = {
    val cluster = new ArrayBuffer[DBSCANPoint]()
    cluster += point
    val queue = mutable.Queue(neighbors)
    while (queue.nonEmpty) {
      for {
        neighbor <- queue.dequeue
        if !visited.contains(neighbor)
      } {
        visited += neighbor
        cluster += neighbor
        val neighborNeighbors = spatialIndex findNeighbors neighbor
        if (neighborNeighbors.length >= minPoints) {
          queue += neighborNeighbors
        }
      }
    }
    Some(cluster)
  }
}

object DBSCAN extends Serializable {
  def apply(eps: Double, minPoints: Int) = {
    new DBSCAN(eps, minPoints)
  }
}
