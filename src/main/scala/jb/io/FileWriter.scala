package jb.io

import jb.model.{IntegratedModel, LabelledCube}
import jb.util.Const

import java.io.{File, PrintWriter}

object FileWriter {

  def writeModelToFile(model: IntegratedModel, filename: String): Unit = {
    val pw = new PrintWriter(new File(Const.MODEL_PREFIX + filename))
    model.cubes
      .foreach { labelledCube => pw.println((labelledCube.min ::: labelledCube.max ::: labelledCube.label :: Nil).mkString(","))}
    pw.flush()
    pw.close()
  }

}
