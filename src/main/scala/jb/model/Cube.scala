package jb.model

import org.apache.spark.ml.linalg

case class Cube(min: List[Double], max: List[Double]) {
  def getMidAsMlVector = new linalg.DenseVector(min.zip(max)
    .map { case (xMin, xMax) => (xMin + xMax) / 2 }
    .toArray)

}

case class CountingCube(min: List[Double], max: List[Double], labelCount: Map[Double, Int]) {
  def isNeighborOf(cube: CountingCube) =
    min.indices
      .forall(index => cube.min(index) == min(index)
        || cube.min(index) == max(index)
        || cube.max(index) == min(index)
        || cube.max(index) == max(index))

  def withDistance(dist: Double) = WeightingCube(min, max, labelCount, dist)

  val getMid = min.zip(max)
    .map { case (xMin, xMax) => (xMin + xMax) / 2 }
}

object CountingCube {
  def fromCube(cube: Cube, labelCount: Map[Double, Int]) = CountingCube(cube.min, cube.max, labelCount)
}

case class WeightingCube(min: List[Double], max: List[Double], labelCount: Map[Double, Int], distance: Double)

case class LabelledCube(min: List[Double], max: List[Double], label: Double) {
  def contains(obj: Array[Double]): Boolean = min.indices
    .forall(index => min(index) <= obj(index) && obj(index) <= max(index))
}
