package com.example.sparktesting

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


trait SparkContextProvider {
  def sc : SparkContext

  def appID: String = (this.getClass.getName + math.floor(math.random * 10E4).toLong.toString)

  def conf = {
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("test")
      .set("spark.ui.enabled", "false")
      .set("spark.app.id", appID)
      .set("spark.driver.host", "localhost")
  }

  def setup(sc: SparkContext): Unit = {
    sc.setCheckpointDir(Utils.createTempDir().toPath.toString)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }


}