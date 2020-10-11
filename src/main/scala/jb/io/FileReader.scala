package jb.io

import jb.conf.Config
import jb.server.SparkEmbedded
import jb.util.Const._
import org.apache.spark.sql.DataFrame

object FileReader {

  def getRawInput(path: String, format: String): DataFrame = {
    val input = SparkEmbedded.ss.read.option("inferSchema", "true").format(format).load(path)
    input.withColumnRenamed(input.columns.last, SPARSE_LABEL).limit(Config.datasetSize)
  }

}
