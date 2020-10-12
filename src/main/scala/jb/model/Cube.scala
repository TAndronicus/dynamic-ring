package jb.model

import jb.conf.Config
import org.apache.spark.ml.linalg.Vectors.dense

case class Cube(min: List[Double], max: List[Double], objects: List[(List[Double], Double)]) {
  def getMidAsMlVector = {
    dense(objects
      .map(_._1)
      .foldLeft(List(0d, 0d))((l1: List[Double], l2: List[Double]) => List(l1.head + l2.head, l1.tail.head + l2.tail.head))
      .map(_ / objects.size)
      .toArray)
  }
}

case class CountingCube(min: List[Double], max: List[Double], mid: List[Double], objects: List[(List[Double], Double)], labelCount: Map[Double, Int]) {
  def isBalanced(numOfLabels: Int) = {
    val grouped = objects.groupBy(_._2)
      .mapValues(_.size / objects.size)
    numOfLabels == grouped.size && grouped.minBy(_._2)._2 > Config.nonBalancedThreshold
  }

  def isNeighborOf(cube: CountingCube) =
    min.indices
      .forall(index => cube.min(index) == min(index)
        || cube.min(index) == max(index)
        || cube.max(index) == min(index)
        || cube.max(index) == max(index))

  def withDistance(dist: Double) = WeightingCube(min, max, labelCount, dist)
}

object CountingCube {
  def fromCube(cube: Cube, labelCount: Map[Double, Int]) = CountingCube(cube.min, cube.max, List(), cube.objects, labelCount) // TODO
}

case class WeightingCube(min: List[Double], max: List[Double], labelCount: Map[Double, Int], distance: Double)

case class LabelledCube(min: List[Double], max: List[Double], label: Double) {
  def contains(obj: Array[Double]): Boolean = min.indices
    .forall(index => min(index) <= obj(index) && obj(index) <= max(index))
}
