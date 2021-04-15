package jb

import jb.conf.Config
import jb.io.FileReader.getRawInput
import jb.parser.TreeParser
import jb.prediction.Predictions.predictBaseClfs
import jb.scaler.FeatureScalers
import jb.selector.FeatureSelectors
import jb.server.SparkEmbedded
import jb.tester.FullTester.{testI, testMv, testRF}
import jb.util.Util
import jb.util.Util._
import jb.vectorizer.FeatureVectorizers.getFeatureVectorizer
import org.apache.spark.ml.Pipeline

class Runner(val nClassif: Int, var nFeatures: Int) {

  def calculateMvIScores(filename: String): Array[Double] = {

    SparkEmbedded.ss.sqlContext.clearCache()

    var input = getRawInput(filename, "csv")
    val densified = densifyLabel(input)
    input = densified._2
    if (nFeatures > input.columns.length - 1) {
      this.nFeatures = input.columns.length - 1
      println(s"Setting nFeatures to $nFeatures")
    }
    val featureVectorizer = getFeatureVectorizer(input.columns)
    val featureSelector = FeatureSelectors.get_chi_sq_selector(nFeatures)
    val scaler = FeatureScalers.minMaxScaler
    val dataPrepPipeline = new Pipeline().setStages(Array(featureVectorizer, featureSelector, scaler))
    val dataPrepModel = dataPrepPipeline.fit(input)
    input = optimizeInput(input, dataPrepModel)

    val nSubsets = nClassif + 2
    val subsets = Util.baggingDatasets(input, nSubsets)
    recacheInput2Subsets(input, subsets)
    val (trainingSubsets, validationSubset, testSubset) = dispenseSubsets(subsets)
    val trainingSubset = unionSubsets(trainingSubsets)

    val baseModels = trainingSubsets.map(subset => getEmptyDT.fit(subset))

    val testedSubset = predictBaseClfs(baseModels, testSubset)
    val mvQualityMeasure = testMv(testedSubset, nClassif)
    val rfQualityMeasure = testRF(trainingSubset, testSubset, nClassif)

    val integratedModel = new TreeParser(
      Config.metricFunction,
      Config.mappingFunction
    ).composeTree(baseModels.toList, validationSubset, Util.getSelectedFeatures(dataPrepModel), densified._1)
    if (Config.logging) integratedModel.checkDiversity(filename)

    val iPredictions = integratedModel.transform(testedSubset)
    val iQualityMeasure = testI(iPredictions, testedSubset)

    clearCache(subsets)

    Array(mvQualityMeasure, rfQualityMeasure, iQualityMeasure)
      .flatMap(_.toArray)

  }

}
