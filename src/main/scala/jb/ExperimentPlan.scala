package jb

import jb.server.SparkEmbedded
import jb.util.Const

import java.nio.file.{Files, Paths}

object ExperimentPlan {

  def main(args: Array[String]): Unit = {
    SparkEmbedded.setLogError()
    val nClassifs = Array(7, 9)
    val nFeatures = 2
    createResultFolder()

    for (nC <- nClassifs) {
      MultiRunner.run(nC, nFeatures)
    }
  }

  def createResultFolder(): Unit = {
    if (!Files.exists(Paths.get(Const.RESULT_PREFIX))) Files.createDirectory(Paths.get(Const.RESULT_PREFIX))
  }

}
