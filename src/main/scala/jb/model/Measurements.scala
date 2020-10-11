package jb.model

case class Measurements(
                         acc: Double,
                         precissionMi: Double,
                         recallMi: Double,
                         fScoreMi: Double,
                         precissionM: Double,
                         recallM: Double,
                         fScoreM: Double
                       ) {
  def toArray: Array[Double] = Array(
    acc: Double,
    precissionMi: Double,
    recallMi: Double,
    fScoreMi: Double,
    precissionM: Double,
    recallM: Double,
    fScoreM: Double
  )
}

object Measurements {
  val numberOfMetrics = 7

  def integratedQuality(array: Array[Array[Double]]): Double = array
    .map(a => if (a(2 * numberOfMetrics) > a(numberOfMetrics) || a(2 * numberOfMetrics) > a(0)) 1 else 0)
    .sum.toDouble / array.length
}
