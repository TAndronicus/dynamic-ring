package jb.conf

import jb.model.CountingCube
import jb.util.functions.{MetricFunctions, WeightingFunctions}

object Config {

  /** Models */
  val maxDepth: Int = 10
  val impurity = "gini"
  val maxBins = 64

  /** Datasets */
  val nonBalancedThreshold = .05
  val sampleFraction = 1

  /** Parametrizing */
  val metricFunction: (CountingCube, CountingCube) => Double = MetricFunctions.euclidean
  val mappingFunction: Map[Double, Map[Double, Int]] => Double = WeightingFunctions.halfByDist

  /** Result catcher */
  val batch: Int = 10 // minimal number of results to average
  val treshold: Double = .4
  val minIter: Int = 10
  val maxIter: Int = 100

  /** Other */
  val recalculate = false
  val logging = true

}
