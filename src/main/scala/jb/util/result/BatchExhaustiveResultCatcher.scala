package jb.util.result

import jb.conf.Config
import jb.model.Measurements

import java.io.{File, PrintWriter}

class BatchExhaustiveResultCatcher(val treshold: Double, val batchSize: Int, val maxIter: Int) extends ResultCatcher {

  var container = Array[Array[Array[Double]]]()
  var currentIndex = 0

  def consume(scores: Array[Array[Double]]): Unit = {
    if (isValid(scores)) {
      container :+= scores
      println("Accepted")
    } else {
      println("Rejected")
    }
    currentIndex += 1
  }

  override def isValid(scores: Array[Array[Double]]): Boolean =
    Measurements.integratedQuality(scores) >= Config.treshold

  def isFilled: Boolean = isFull

  def isFull: Boolean = {
    currentIndex >= maxIter
  }

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

  def sumRelativeDelta(score: Array[Array[Double]]) = score.map(ar => (ar.last - ar.head) / ar.head).sum

  def writeScores(finalScores: Array[Array[Double]], args: Array[String]): Unit = {
    val pw = new PrintWriter(new File("result"))
    finalScores.foreach(scores => pw.println(scores.map(_.toString).reduce((s1, s2) => s1 + "," + s2)))
    pw.flush()
    pw.close()
  }

}
