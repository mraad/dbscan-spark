package com.esri.dbscan

object DBSCANStatus extends Enumeration {
  type DBSCANStatus = Value
  val NOISE, CLASSIFIED, UNCLASSIFIED = Value
}
