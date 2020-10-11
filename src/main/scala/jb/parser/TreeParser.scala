package jb.parser

import jb.model._
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.tree.{ContinuousSplit, InternalNode, LeafNode, Node}

class TreeParser(
                  metricFunction: (CountingCube, CountingCube) => Double,
                  mappingFunction: Map[Double, Map[Double, Int]] => Double
                ) {

  private val pairWithNeigbors = (cubes: List[CountingCube]) =>
    (for {
      cube <- cubes
      neighbor <- cubes if cube isNeighborOf neighbor
    } yield (cube, neighbor.withDistance(metricFunction(cube, neighbor)))) // List[(CountingCube, WeightingCube)]
      .groupBy { case (center, _) => center } // Map[CountingCube, List[(CountingCube, WeightingCube)]], turn to map
      .mapValues(_.map(_._2)) // Map[CountingCube, List[WeightingCube]], list of neighbors as value
  private val voteForLabel = (cubes: Map[CountingCube, List[WeightingCube]]) =>
    cubes
      .mapValues(_.map(wc => (wc.distance, wc.labelCount)).toMap)
      .mapValues(mappingFunction)
      .map { case (cc, label) => LabelledCube(cc.min, cc.max, label) }
      .toList
  private val extractCubes = (trees: List[DecisionTreeClassificationModel]) => {
    val (x1cutpoints, x2cutpoints) = trees.map(_.rootNode)
      .flatMap(extractCutpointsRecursively)
      .distinct
      .partition({ case (feature, _) => feature == 0 })
    cutpointsCrossProd(
      extractCutpointsFromPartitions(x1cutpoints),
      extractCutpointsFromPartitions(x2cutpoints)
    )
      .map { case ((minX1, maxX1), (minX2, maxX2)) => Cube(List(minX1, minX2), List(maxX1, maxX2)) }
      .map(cube => CountingCube.fromCube(cube, classifyMid(cube, trees)))
  }

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

  def composeTree(trees: List[DecisionTreeClassificationModel]) = {
    new IntegratedModel((extractCubes andThen pairWithNeigbors andThen voteForLabel) (trees))
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
