package jb.parser

import jb.conf.Config
import jb.model._
import jb.util.{Const, Util}
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.ml.tree.{ContinuousSplit, InternalNode, LeafNode, Node}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class TreeParser(
                  metricFunction: (CountingCube, CountingCube) => Double,
                  mappingFunction: Map[Double, Map[Double, Int]] => Double
                ) {

  def composeTree(trees: List[DecisionTreeClassificationModel], validationDataset: DataFrame, selected: Array[Int], numOfLabels: Int) = {
    val countingCubes = extractCubes(trees, validationDataset, selected)
    val neighborMap = pairWithNeigbors(countingCubes, numOfLabels)
    if (Config.logging && !neighborMap.exists { case (mid, neighbors) => neighbors.filter(wc => !(wc.min == mid.min && wc.max == mid.max)).exists(_.balanced) }) println("Highly imbalanced")
    val labelledCubes = voteForLabel(neighborMap)
    new IntegratedModel(labelledCubes)
  }

  private def pairWithNeigbors: (List[CountingCube], Int) => Map[CountingCube, List[WeightingCube]] = (cubes: List[CountingCube], numOfLabels: Int) =>
    (for {
      cube <- cubes
      neighbor <- cubes if cube isNeighborOf neighbor
    } yield (cube, neighbor.withDistanceAndBalanced(metricFunction(cube, neighbor), neighbor.isBalanced(numOfLabels)))) // List[(CountingCube, WeightingCube)]
      .groupBy { case (center, _) => center } // Map[CountingCube, List[(CountingCube, WeightingCube)]], turn to map
      .mapValues(_.map(_._2)) // Map[CountingCube, List[WeightingCube]], list of neighbors as value

  private def voteForLabel: Map[CountingCube, List[WeightingCube]] => List[LabelledCube] = (cubes: Map[CountingCube, List[WeightingCube]]) =>
    cubes
      .mapValues(_.filter(wc => wc.balanced || wc.distance == 0))
      .mapValues(_.map(wc => (wc.distance, wc.labelCount)).toMap)
      .mapValues(mappingFunction)
      .map { case (cc, label) => LabelledCube(cc.min, cc.max, label) }
      .toList

  private def extractCutpointsRecursively(tree: Node): List[Tuple2[Int, Double]] = {
    tree match {
      case _: LeafNode => List()
      case branch: InternalNode => branch.split match {
        case contSplit: ContinuousSplit =>
          (extractCutpointsRecursively(branch.leftChild)
            ::: ((contSplit.featureIndex -> contSplit.threshold)
            :: extractCutpointsRecursively(branch.rightChild)))
        case _ => throw new Exception("Unsupported split")
      }
      case _ => throw new Exception("Unsupported node")
    }
  }

  private def extractCubes: (List[DecisionTreeClassificationModel], DataFrame, Array[Int]) => List[CountingCube] = (trees: List[DecisionTreeClassificationModel], validationDataset: DataFrame, selected: Array[Int]) => {
    val (x1cutpoints, x2cutpoints) = trees.map(_.rootNode)
      .flatMap(extractCutpointsRecursively)
      .distinct
      .partition({ case (feature, _) => feature == 0 })
    cutpointsCrossProd(
      extractCutpointsFromPartitions(x1cutpoints),
      extractCutpointsFromPartitions(x2cutpoints)
    ) // List[((Double, Double), (Double, Double))]
      .map { case ((minX1, maxX1), (minX2, maxX2)) =>
        Cube(
          List(minX1, minX2),
          List(maxX1, maxX2),
          validationDataset.
            filter(col(s"_c${selected(0)}") >= minX1
              && col(s"_c${selected(0)}") <= maxX1
              && col(s"_c${selected(1)}") >= minX2
              && col(s"_c${selected(1)}") <= maxX2
            )
            .select(vector_to_array(col(Const.FEATURES)), col(Const.LABEL))
            .collect()
            .map(r => (r.getSeq[Double](0).toList, Util.parseDouble(r.get(1))))
            .toList
        )
      } // List[Cube]
      //TODO: mid obliczany dwukrotnie
      .map(cube => CountingCube.fromCube(cube, classifyMid(cube, trees))) // List[CountingCube]
  }

  private def classifyMid(cube: Cube, trees: List[DecisionTreeClassificationModel]) = trees
    .map(_.predict(cube.getMidAsMlVector))
    .groupBy(identity)
    .mapValues(_.size)

  private def extractCutpointsFromPartitions(cutpointPartition: List[Tuple2[Int, Double]]) = cutpointPartition
    .map { case (_, value) => value }

  private def cutpointsCrossProd(x1cutpoints: List[Double], x2cutpoints: List[Double]) =
    crossProd(
      pairNeighbors(withLimits(x1cutpoints)),
      pairNeighbors(withLimits(x2cutpoints))
    )

  private def withLimits(l: List[Double]): List[Double] = 0.0 :: l.sorted ::: 1.0 :: Nil

  private def pairNeighbors(l: List[Double]) = l.sliding(2, 1)
    .map(n => (n(0), n(1)))
    .toList

  private def crossProd[A](l1: List[A], l2: List[A]) = for {
    x1 <- l1
    x2 <- l2
  } yield (x1, x2)

}
