package jb

import jb.conf.Config
import jb.util.Const
import jb.util.Const.FILENAME_PREFIX
import jb.util.result.{GeneralCatcher, ResultCatcher}

import java.io.File

object MultiRunner {


  def run(nClassif: Int, nFeatures: Int): Unit = {
    val filenames = new File(Const.FILENAME_PREFIX)
      .listFiles()
      .map(_.getName)

    val runner = new Runner(nClassif, nFeatures)
    val resultCatcher = runForFiles(runner)(filenames)

    resultCatcher.writeScores(Array(nClassif.toString))
  }

  private def runForFiles(runner: Runner)(filenames: Array[String]): ResultCatcher = {
    val resultCatcher = getResultCatcher
    while (resultCatcher.canConsume && !resultCatcher.isFull) {
      val scores = new Array[Array[Double]](filenames.length)
      for (index <- filenames.indices) {
        println(filenames(index))
        scores(index) = runner.calculateMvIScores(FILENAME_PREFIX + filenames(index))
      }
      resultCatcher.consume(scores)
    }
    resultCatcher
  }

  private def getResultCatcher: ResultCatcher = {
    new GeneralCatcher(Config.treshold, Config.batch, Config.minIter, Config.maxIter)
  }

}
