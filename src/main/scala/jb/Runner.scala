package jb

import java.util.stream.IntStream

import jb.conf.Config
import jb.io.FileReader.getRawInput
import jb.parser.TreeParser
import jb.prediction.Predictions.predictBaseClfs
import jb.scaler.FeatureScalers
import jb.selector.FeatureSelectors
import jb.server.SparkEmbedded
import jb.tester.FullTester.{testI, testMv, testRF}
import jb.util.Util._
import jb.vectorizer.FeatureVectorizers.getFeatureVectorizer
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.{col, max, min}

class Runner(val nClassif: Int, var nFeatures: Int) {

  def calculateMvIScores(filename: String): Array[Double] = {

    //    import SparkEmbedded.ss.implicits._
    SparkEmbedded.ss.sqlContext.clearCache()

    var input = getRawInput(filename, "csv")
    input = densifyLabel(input)
//    input.groupBy(col("label")).count().select(max(col("count")).divide(min(col("count")))).show()
//    println(input.count())
//    input.groupBy(col("label")).count().select(min(col("count"))).show()
//    input.groupBy(col("label")).count().select(max(col("count"))).show()
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

    val nSubsets = nClassif + 1
    val subsets = input.randomSplit(IntStream.range(0, nSubsets).mapToDouble(_ => 1D / nSubsets).toArray)
    recacheInput2Subsets(input, subsets)
    val (trainingSubsets, testSubset) = dispenseSubsets(subsets)
    val trainingSubset = unionSubsets(trainingSubsets)

    val baseModels = trainingSubsets.map(subset => getEmptyDT.fit(subset))

    val testedSubset = predictBaseClfs(baseModels, testSubset)
    val mvQualityMeasure = testMv(testedSubset, nClassif)
    val rfQualityMeasure = testRF(trainingSubset, testSubset, nClassif)

    val integratedModel = new TreeParser(
      Config.metricFunction,
      Config.mappingFunction
    ).composeTree(baseModels.toList)
    integratedModel.checkDiversity(filename)

    val iPredictions = integratedModel.transform(testedSubset)
    val iQualityMeasure = testI(iPredictions, testedSubset)

    clearCache(subsets)

    Array(mvQualityMeasure, rfQualityMeasure, iQualityMeasure)
      .flatMap(_.toArray)

  }

}
