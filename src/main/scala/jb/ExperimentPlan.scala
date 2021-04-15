package jb

import jb.conf.Config
import jb.server.SparkEmbedded
import jb.util.Const

import java.nio.file.{Files, Paths}

object ExperimentPlan {

  def main(args: Array[String]): Unit = {
    SparkEmbedded.setLogError()
    val nFeatures = 2
    createResultFolder()

    for (nC <- Config.numberOfBaseClassifiers) {
      MultiRunner.run(nC, nFeatures)
    }
  }

  def createResultFolder(): Unit = {
    if (!Files.exists(Paths.get(Const.RESULT_PREFIX))) Files.createDirectory(Paths.get(Const.RESULT_PREFIX))
  }

}
