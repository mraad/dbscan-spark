package com.esri.dbscan

/**
  * Cell in a fishnet represented by a row and a column value.
  */
case class Cell(val row: Int, val col: Int) {

  /**
    * Get the envelope of the cell
    *
    * @param cellSize the cell size
    * @return the cell envelope
    */
  def toEnvp(cellSize: Double): Envp = {
    val xmin = col * cellSize
    val ymin = row * cellSize
    val xmax = xmin + cellSize
    val ymax = ymin + cellSize
    Envp(xmin, ymin, xmax, ymax)
  }

}
