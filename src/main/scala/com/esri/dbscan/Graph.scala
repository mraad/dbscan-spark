package com.esri.dbscan

/**
  * Companion object to create new immutable Graph instance.
  */
object Graph extends Serializable {
  /**
    * Create a new immutable Graph where vertices are of type <code>T</code>
    *
    * @tparam T the vertex type.
    * @return a new Graph instance.
    */
  def apply[T]() = new Graph[T](Map.empty[T, Set[T]])
}

/**
  * Graph implemented as a Map[vertex,set of out vertexes]
  *
  * @param vertices initial map of vertices and their associated output verticies.
  * @tparam T the vertex type.
  */
class Graph[T](vertices: Map[T, Set[T]]) extends Serializable {

  /**
    * Create a new Graph by adding supplied map.
    * This method assists in the double dispatching with graphC = graphA + graphB
    *
    * @param map a map of verticies and their output verticies
    * @return a new Graph instance.
    */
  def +(map: Map[T, Set[T]]): Graph[T] = {
    val merged = vertices ++ map.map {
      case (k, v) => k -> (v ++ vertices.getOrElse(k, Set.empty[T]))
    }
    new Graph[T](merged)
  }

  /**
    * Create a new Graph by adding supplied graph.
    *
    * @param that the graph to add.
    * @return a new Graph instance.
    */
  def +(that: Graph[T]): Graph[T] = {
    that + vertices
  }

  /**
    * Add a one way edge between supplied origin vertex to destination vertex
    *
    * @param orig the 'from' vertex
    * @param dest the 'to' vertex
    * @return a new Graph instance
    */
  def addOneWay(orig: T, dest: T): Graph[T] = {
    vertices.get(orig) match {
      case None =>
        new Graph[T](vertices + (orig -> Set(dest)))
      case Some(prev) =>
        if (prev.contains(dest)) this else new Graph[T](vertices + (orig -> (prev + dest)))
    }
  }

  /**
    * Add a two way edge between supplied origin vertex to destination vertex
    *
    * @param orig the 'from' vertex
    * @param dest the 'to' vertex
    * @return a new Graph instance
    */
  def addTwoWay(orig: T, dest: T): Graph[T] = {
    addOneWay(orig, dest).addOneWay(dest, orig)
  }

  /**
    * http://stackoverflow.com/questions/5471234/how-to-implement-a-dfs-with-immutable-data-types
    *
    * @param start the starting vertex
    * @return list of reachable vertices
    */
  def traverse(start: T): List[T] = {
    def childrenNotVisited(parent: T, visited: List[T]) =
      vertices(parent) filter (x => !visited.contains(x))

    @annotation.tailrec
    def loop(stack: Set[T], visited: List[T]): List[T] = {
      if (stack isEmpty) visited
      else loop(childrenNotVisited(stack.head, visited) ++ stack.tail, stack.head :: visited)
    }

    loop(Set(start), Nil) // reverse
  }

  /**
    * Give sequence of connected vertices a unique global identifier.
    *
    * @return a dictionary (map) where the key is the vertex and the value is the associated global identifier.
    */
  def assignGlobalID(): Map[T, Int] = {
    val (_, map, _) = vertices
      .keys
      .foldLeft((1, Map[T, Int](), Set[T]())) {
        case ((prevID, map, set), vertex) => {
          if (set.contains(vertex)) {
            (prevID, map, set)
          } else {
            val list = traverse(vertex)
            (prevID + 1, map ++ list.map(_ -> prevID), set ++ list)
          }
        }
      }
    map
  }

  /**
    * @return text representation of this instance.
    */
  override def toString(): String = s"Graph($vertices)"
}
