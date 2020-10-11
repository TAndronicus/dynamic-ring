package jb.server

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkEmbedded {

  val conf = new SparkConf().setAppName("dtree-merge").setMaster("local")
  val ss = SparkSession.builder.config(conf).getOrCreate

  def setLogWarn(): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
  }

  def setLogError(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
  }

}
