package jb

import jb.server.SparkEmbedded

object ExperimentPlan {

  def main(args: Array[String]): Unit = {
    SparkEmbedded.setLogError()
    val nClassifs = Array(7, 9)
    val nFeatures = 2

    for (nC <- nClassifs) {
      MultiRunner.run(nC, nFeatures)
    }
  }

}
