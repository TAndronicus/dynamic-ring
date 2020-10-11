package jb.util

import jb.conf.Config
import jb.server.SparkEmbedded
import jb.util.Const._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.feature.ChiSqSelectorModel
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable

object Util {

  def densifyLabel(input: DataFrame): DataFrame = {
    val columnMapping = input.select(Const.SPARSE_LABEL)
      .orderBy(Const.SPARSE_LABEL)
      .dropDuplicates()
      .collect()
      .map(_.get(0).toString)
      .zipWithIndex
      .toMap
    val mapper = columnMapping(_)
    input.withColumn(Const.LABEL, udf(mapper)
      .apply(col(SPARSE_LABEL)))
      .drop(Const.SPARSE_LABEL)
  }

  def optimizeInput(input: DataFrame, dataPrepModel: PipelineModel): DataFrame = {
    val selected = Util.getSelectedFeatures(dataPrepModel)
    dataPrepModel.transform(input).select(
      col(FEATURES),
      col(LABEL),
      vector_to_array(col(FEATURES)).getItem(0).alias(s"_c${selected(0)}"),
      vector_to_array(col(FEATURES)).getItem(1).alias(s"_c${selected(1)}")
    ).persist
  }

  def getSelectedFeatures(dataPrepModel: PipelineModel): Array[Int] = {
    dataPrepModel.stages(1).asInstanceOf[ChiSqSelectorModel].selectedFeatures
  }

  def recacheInput2Subsets(input: DataFrame, subsets: Array[DataFrame]): Unit = {
    input.unpersist
    subsets.foreach(_.cache)
  }

  def clearCache(subsets: Array[Dataset[Row]]) = {
    subsets.foreach(_.unpersist)
  }

  /**
    * Returns a tuple with array of training subsets, cv subset & testing subset
    *
    * @param subsets - array of subsets to dispense
    * @return
    */
  def dispenseSubsets(subsets: Array[DataFrame]): (Array[DataFrame], DataFrame) = {
    val trainingSubsets = subsets.take(subsets.length - 1)
    val testSubset = subsets.last
    (trainingSubsets, testSubset)
  }

  def unionSubsets(subsets: Array[DataFrame]): DataFrame = {
    subsets.reduce((s1, s2) => s1.union(s2))
  }

  def getElCubeSize(mins: Array[Double], maxes: Array[Double], division: Int): Array[Double] = {
    mins.indices.map(i => (maxes(i) - mins(i)) / division).toArray
  }

  def calculateMomentsByLabels(input: DataFrame, selectedFeatures: Array[Int]): Map[Double, Array[Double]] = {
    val selFNames = selectedFeatures.map(item => COL_PREFIX + item)
    val intermediate = input.select(selFNames.map(col).+:(col(LABEL)): _*).groupBy(col(LABEL)).avg(selFNames: _*)
    val moments = mutable.Map[Double, Array[Double]]()
    for (row <- intermediate.collect()) {
      moments.put(parseDouble(row.get(0)), row.toSeq.takeRight(row.length - 1).toArray.map(parseDouble))
    }
    moments.toMap
  }

  /**
    * @param input            - dataset
    * @param selectedFeatures - columns to aggregate
    * @param baseModels       - models to predict
    * @return moments - map with labels as keys and moments (coordinates) as values
    */
  def calculateMomentsByPredictionCollectively(
                                                input: DataFrame,
                                                selectedFeatures: Array[Int],
                                                baseModels: Array[DecisionTreeClassificationModel],
                                                filterLabels: Boolean): Map[Double, Array[Double]] = {
    val weightedMean = calculateMomentsByPrediction(input, selectedFeatures, baseModels, filterLabels)
    val moments = mutable.Map[Double, Array[Double]]()
    for (row <- weightedMean.collect()) {
      moments.put(parseDouble(row.getDouble(0)), row.toSeq.takeRight(row.length - 2).toArray.map(parseDouble).map(_ / row.getLong(1)))
    }
    weightedMean.unpersist
    moments.toMap
  }

  def parseDouble(value: Any): Double = {
    value match {
      case int: Int =>
        int.toDouble
      case double: Double =>
        double
    }
  }

  /**
    * @param input            dataset
    * @param selectedFeatures columns to aggregate
    * @param baseModels       models to predict
    * @param filterLabels     take into consideration only properly classifier objects
    * @return dataset with summed moments with the following schema:
    *         |-------|-------|---------|---------|
    *         | label | count | sum(x1) | sum(x2) |
    *         |-------|-------|---------|---------|
    *         where sums aggregate averages of different classifiers (sum(avg(clf_1), avg(clf_2), ..., avg(clf_n))
    *
    *         SIDE EFFECT: returned dataframe is cached - it must be unpersisted after processing
    *
    */
  def calculateMomentsByPrediction(
                                    input: DataFrame,
                                    selectedFeatures: Array[Int],
                                    baseModels: Array[DecisionTreeClassificationModel],
                                    filterLabels: Boolean): DataFrame = {
    var dataset = input.select(col("*"))
    val selFNames = selectedFeatures.map(item => COL_PREFIX + item)
    val schema = StructType(Seq(
      StructField(PREDICTION, DoubleType, nullable = false), // predicted label
      StructField(PREDICTION + COUNT_SUFFIX, LongType, nullable = false) // count of objects composing the aggregate
    ) ++ selFNames.map(_ + AVERAGE_SUFFIX).map(item => StructField(item, DoubleType, nullable = true))) // summed values of features
    var aggregate = SparkEmbedded.ss.createDataFrame(SparkEmbedded.ss.sparkContext.emptyRDD[Row], schema)

    for (index <- baseModels.indices) {
      dataset = baseModels(index).setPredictionCol(PREDICTION + "_" + index).transform(dataset).drop(COLUMNS2DROP: _*)
      if (filterLabels) dataset = dataset.where(PREDICTION + "_" + index + "=" + LABEL)
      aggregate = aggregate.union(
        dataset.drop(LABEL).withColumnRenamed(PREDICTION + "_" + index, PREDICTION).groupBy(PREDICTION).agg(count(PREDICTION), selFNames.map(avg): _*)
      )
    }
    aggregate.groupBy(PREDICTION).agg(sum(PREDICTION + COUNT_SUFFIX), selFNames.map(_ + AVERAGE_SUFFIX).map(col).map(_ * col(PREDICTION + COUNT_SUFFIX)).map(sum): _*)
    aggregate.cache
  }

  /**
    * @param input            - array of datasets
    * @param selectedFeatures - columns to aggregate
    * @param baseModels       - models to predict
    * @return moments - map with labels as keys and moments (coordinates) as values
    */
  def calculateMomentsByPredictionRespectively(
                                                input: Array[DataFrame],
                                                selectedFeatures: Array[Int],
                                                baseModels: Array[DecisionTreeClassificationModel],
                                                filterLabels: Boolean): Map[Double, Array[Double]] = {
    val selFNames = selectedFeatures.map(item => COL_PREFIX + item)
    val schema = StructType(Seq(
      StructField(PREDICTION, DoubleType, nullable = false),
      StructField("sum(" + PREDICTION + COUNT_SUFFIX + ")", LongType, nullable = false)
    ) ++ selFNames.map("sum((" + _ + AVERAGE_SUFFIX + " * " + PREDICTION + COUNT_SUFFIX + "))").map(item => StructField(item, DoubleType, nullable = true)))
    var aggregate = SparkEmbedded.ss.createDataFrame(SparkEmbedded.ss.sparkContext.emptyRDD[Row], schema)
    for (index <- baseModels.indices) {
      val baseMoments = calculateMomentsByPrediction(input(index), selectedFeatures, Array(baseModels(index)), filterLabels)
      aggregate = aggregate.union(baseMoments)
      baseMoments.unpersist
    }
    aggregate.cache
    val avgName = (item: String) => "sum((" + item + AVERAGE_SUFFIX + " * " + PREDICTION + COUNT_SUFFIX + "))"
    val sumName = "sum(" + PREDICTION + COUNT_SUFFIX + ")"
    val weightedMoments = aggregate.groupBy(PREDICTION).agg(sum(sumName),
      selFNames.map(avgName).map(col).map(sum): _*)
    val moments = mutable.Map[Double, Array[Double]]()
    for (row <- weightedMoments.collect()) {
      moments.put(parseDouble(row.getDouble(0)), row.toSeq.takeRight(row.length - 2).toArray.map(parseDouble).map(_ / row.getLong(1)))
    }
    aggregate.unpersist
    weightedMoments.unpersist
    moments.toMap
  }

  def getEmptyDT = {
    new DecisionTreeClassifier()
      .setLabelCol(LABEL)
      .setFeaturesCol(FEATURES)
      .setImpurity(Config.impurity)
      .setMaxDepth(Config.maxDepth)
      .setMaxBins(Config.maxBins)
  }

}
