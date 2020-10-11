package jb.scaler

import jb.util.Const
import org.apache.spark.ml.feature.MinMaxScaler

object FeatureScalers {

  val minMaxScaler = new MinMaxScaler().setInputCol(Const.NON_SCALED_FEATURES).setOutputCol(Const.FEATURES)

}
