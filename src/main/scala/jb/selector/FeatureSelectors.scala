package jb.selector

import jb.util.Const._
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.feature.{ChiSqSelector, ChiSqSelectorModel}

object FeatureSelectors {

  def get_chi_sq_selector(nFeatures: Int): Estimator[ChiSqSelectorModel] = {
    new ChiSqSelector()
      .setNumTopFeatures(nFeatures)
      .setFeaturesCol(SPARSE_FEATURES)
      .setLabelCol(LABEL)
      .setOutputCol(NON_SCALED_FEATURES)
  }

}
