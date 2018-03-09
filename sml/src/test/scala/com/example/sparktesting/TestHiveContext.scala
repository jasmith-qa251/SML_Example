package com.example.sparktesting

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

private[example] class TestHiveContext(sc: SparkContext, config: Map[String, String],
                                   localMetastorePath: String, localWarehousePath: String) extends HiveContext(sc) {
  setConf("javax.jdo.option.ConnectionURL",
    s"jdbc:derby:;databaseName=$localMetastorePath;create=true")
  setConf("hive.metastore.warehouse.dir", localWarehousePath)
  override def configure(): Map[String, String] = config
}