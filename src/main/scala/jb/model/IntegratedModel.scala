package jb.model

import jb.util.Const
import org.apache.spark.ml.linalg
import org.apache.spark.sql.DataFrame

class IntegratedModel(cubes: List[LabelledCube]) {

  def transform(dataframe: DataFrame): Array[Double] =
    dataframe.select(Const.FEATURES)
      .collect()
      .map(_.toSeq.head)
      .map {
        case v: linalg.Vector => transform(v.toArray)
        case _ => throw new Exception("Cannot be parsed")
      }

  def transform(obj: Array[Double]): Double =
    cubes.find(_.contains(obj)) match {
      case c: Some[LabelledCube] => c.head.label
      case _ => throw new Exception("Cube not found: (" + obj.mkString(", ") + ")")
    }

  def checkDiversity(filename: String): Unit = {
    if (cubes.map(_.label).distinct.size == 1)
      println(s"Single label in $filename")
  }

}
