package jb.util.functions

import jb.model.CountingCube

object MetricFunctions {

  val constant: (CountingCube, CountingCube) => Double = (_, _) => 0 // TODO: refactor constant functions to typeclasses & implicits

  val euclidean: (CountingCube, CountingCube) => Double = (cube, neighbor) => math.sqrt(
    cube.mid.zip(neighbor.mid)
      .map { case (cubeMid, nMid) => math.pow(cubeMid - nMid, 2) }
      .sum
  )

  val euclideanSquared: (CountingCube, CountingCube) => Double = (cube, neighbor) => cube.mid.zip(neighbor.mid)
    .map { case (cubeMid, nMid) => math.pow(cubeMid - nMid, 2) }
    .sum

  val euclideanMod: Int => (CountingCube, CountingCube) => Double = pow => (cube, neighbor) => cube.mid.zip(neighbor.mid)
    .map { case (cubeMid, nMid) => math.pow(cubeMid - nMid, pow) }
    .sum

  val manhattan: (CountingCube, CountingCube) => Double = (cube, neighbor) => cube.mid.zip(neighbor.mid)
    .map { case (cubeMid, nMid) => math.abs(cubeMid - nMid) }
    .sum

  val chebyschev: (CountingCube, CountingCube) => Double = (cube, neighbor) => cube.mid.zip(neighbor.mid)
    .map { case (cubeMid, nMid) => math.abs(cubeMid - nMid) }
    .max

}
