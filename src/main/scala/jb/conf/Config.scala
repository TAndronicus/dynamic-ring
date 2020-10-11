package jb.conf

import jb.model.CountingCube
import jb.util.functions.{MetricFunctions, WeightingFunctions}

object Config {

  /** Models */
  val maxDepth: Int = 10
  val impurity = "gini"
  val maxBins = 64
  val datasetSize = 10000

  /** Parametrizing */
  val metricFunction: (CountingCube, CountingCube) => Double = MetricFunctions.euclidean
  val mappingFunction: Map[Double, Map[Double, Int]] => Double = WeightingFunctions.halfByDist

  /** Result catcher */
  val treshold: Double = .4
  val batch: Int = 4
  val minIter: Int = 10
  val maxIter: Int = 200

  /** Other */
  val recalculate = false

}
