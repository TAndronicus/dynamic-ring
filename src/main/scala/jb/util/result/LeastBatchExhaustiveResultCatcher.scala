package jb.util.result

import jb.conf.Config
import jb.model.Measurements
import jb.util.Const.RESULT_PREFIX

import java.io.{File, PrintWriter}

class LeastBatchExhaustiveResultCatcher(val treshold: Double, val batchSize: Int, val minIter: Int, val maxIter: Int) extends ResultCatcher {

  override val header: Array[String] = for {
    meas <- Measurements.namesOfMetrics
    prefix <- Array("mv", "rf", "i")
  } yield s"${prefix}_$meas"
  var container: Array[Array[Array[Double]]] = Array[Array[Array[Double]]]()
  var currentIndex = 0

  def consume(scores: Array[Array[Double]]): Unit = {
    if (isValid(scores)) {
      container :+= scores
      println(s"$currentIndex: Accepted (${container.length}/$batchSize)")
    } else {
      println(s"$currentIndex: Rejected")
    }
    currentIndex += 1
  }

  override def isValid(scores: Array[Array[Double]]): Boolean =
    Measurements.integratedQuality(scores) >= Config.treshold

  def isFull: Boolean = {
    currentIndex >= maxIter || (isFilled && currentIndex >= minIter)
  }

  def isFilled: Boolean = container.length >= batchSize

  def aggregate: Array[Array[Double]] = {
    if (container.length >= batchSize) {
      val sortedContainer =
        if (container.length != batchSize) container.sortWith((ar1, ar2) => sumRelativeDelta(ar1) > sumRelativeDelta(ar2)).take(batchSize)
        else container
      val result = new Array[Array[Double]](sortedContainer(0).length)
      for (midIndex <- sortedContainer(0).indices) {
        val partialResult = new Array[Double](sortedContainer(0)(0).length)
        for (innerIndex <- sortedContainer(0)(0).indices) {
          partialResult(innerIndex) = sortedContainer.map(score => score(midIndex)(innerIndex)).sum / batchSize
        }
        result(midIndex) = partialResult
      }
      println(s"Consumed: ${container.length}/$maxIter, retrieved: $batchSize")
      result
    } else {
      throw new RuntimeException("Not filled")
    }
  }

  def sumRelativeDelta(score: Array[Array[Double]]): Double = {
    score.map(ar => ar(0) + ar(Measurements.numberOfMetrics) - 2 * ar(2 * Measurements.numberOfMetrics)).sum
  }

  def writeScores(finalScores: Array[Array[Double]], args: Array[String]): Unit = {
    val filename = RESULT_PREFIX + args.reduce((s1, s2) => s1 + "_" + s2)
    writeToFile(finalScores, filename)
  }

  private def writeToFile(finalScores: Array[Array[Double]], filename: String) = {
    val pw = new PrintWriter(new File(filename))
    pw.println(header.mkString(","))
    finalScores.foreach(scores => pw.println(scores.map(_.toString).reduce((s1, s2) => s1 + "," + s2)))
    pw.flush()
    pw.close()
  }

}
