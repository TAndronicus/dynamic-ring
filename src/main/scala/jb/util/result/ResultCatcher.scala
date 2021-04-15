package jb.util.result

trait ResultCatcher {

  val header: Array[String] = Array.empty

  def consume(scores: Array[Array[Double]]): Unit

  def isFilled: Boolean

  def isFull: Boolean

  def canConsume: Boolean = !isFull

  def aggregate: Array[Array[Double]]

  def writeScores(finalScores: Array[Array[Double]], args: Array[String]): Unit

  def writeScores(args: Array[String]): Unit = writeScores(aggregate, args)

  def isValid(scores: Array[Array[Double]]): Boolean

}
