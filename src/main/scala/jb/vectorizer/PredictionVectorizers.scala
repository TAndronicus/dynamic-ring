package jb.vectorizer

import jb.util.Const.{PREDICTION, SPARSE_PREDICTIONS}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.VectorAssembler

object PredictionVectorizers {

  def getPredictionVectorizer(nClassif: Int): Transformer = {
    val cols = for (i <- 0.until(nClassif)) yield PREDICTION + "_" + i
    new VectorAssembler().setInputCols(cols.toArray).setOutputCol(SPARSE_PREDICTIONS)
  }

}
