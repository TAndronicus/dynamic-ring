package jb.model

import jb.conf.Config

case class Cube(min: List[Double], max: List[Double], objects: List[(List[Double], Double)])

case class CountingCube(min: List[Double], max: List[Double], mid: List[Double], objects: List[(List[Double], Double)], labelCount: Map[Double, Int]) {
  def isBalanced(numOfLabels: Int) = {
    val grouped = objects.groupBy(_._2)
      .mapValues(_.size.toDouble / objects.size)
    numOfLabels == grouped.size && grouped.minBy(_._2)._2 > Config.nonBalancedThreshold
  }

  def isNeighborOf(cube: CountingCube) =
    min.indices
      .forall(index => cube.min(index) == min(index)
        || cube.min(index) == max(index)
        || cube.max(index) == min(index)
        || cube.max(index) == max(index))

  def withDistanceAndBalanced(dist: Double, balanced: Boolean) = WeightingCube(min, max, labelCount, dist, balanced)
}

object CountingCube {
  def fromCube(cube: Cube, labelCount: Map[Double, Int], mid: List[Double]) = CountingCube(
    cube.min,
    cube.max,
    mid,
    cube.objects,
    labelCount
  )
}

case class WeightingCube(min: List[Double], max: List[Double], labelCount: Map[Double, Int], distance: Double, balanced: Boolean)

case class LabelledCube(min: List[Double], max: List[Double], label: Double) {
  def contains(obj: Array[Double]): Boolean = min.indices
    .forall(index => min(index) <= obj(index) && obj(index) <= max(index))
}
