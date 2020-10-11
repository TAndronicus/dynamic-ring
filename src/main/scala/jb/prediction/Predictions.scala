package jb.prediction

import jb.util.Const._
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.sql.DataFrame

object Predictions {

  def predictBaseClfs(baseModels: Array[DecisionTreeClassificationModel], testSubset: DataFrame): DataFrame = {
    var dataset = testSubset
    for (i <- baseModels.indices) {
      dataset = baseModels(i).setPredictionCol(PREDICTION + "_" + i).transform(dataset).drop(COLUMNS2DROP: _*)
    }
    dataset
  }

}
