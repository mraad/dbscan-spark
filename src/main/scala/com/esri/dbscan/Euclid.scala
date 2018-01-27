package com.esri.dbscan

/**
  * A 2D location with identification.
  */
trait Euclid extends Serializable {
  /**
    * @return the identification.
    */
  def id(): Long

  /**
    * @return the horizontal position.
    */
  def x(): Double

  /**
    * @return the vertical position.
    */
  def y(): Double

  /**
    * Check if supplied point is the same as this point. This is a naive implementation as it checks only the point identifier.
    *
    * @param other the other point.
    * @return if the other point id is the same as this point id.
    */
  override def equals(other: Any): Boolean = other match {
    case that: Euclid => id == that.id
    case _ => false
  }

  /**
    * Hash representation of this point
    *
    * @return the hash of the point id.
    */
  override def hashCode(): Int = {
    Smear.smear(id.toInt)
  }

}
