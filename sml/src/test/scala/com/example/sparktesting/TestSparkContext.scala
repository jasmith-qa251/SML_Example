package com.example.sparktesting

import java.io.File

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.{FunSuite, Suite}

import scala.collection.mutable.HashMap

trait TestSparkContext extends FunSuite with SharedSparkContext with  TestSparkContextLike{
  self: Suite =>

  override def beforeAll(): Unit = {
    super.beforeAll()
    super.sqlBeforeAllTestCases()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SQLContextProvider._sqlContext = null
  }
}

trait TestSparkContextLike extends SparkContextProvider with Serializable {

  lazy val _hc: HiveContext = SQLContextProvider._sqlContext

  def sqlBeforeAllTestCases(): Unit = {
    try {
      val tempDir = Utils.createTempDir()
      val localMetastorePath = new File(tempDir, "metastore").getCanonicalPath
      val localWarehousePath = new File(tempDir, "warehouse").getCanonicalPath

      def newTemporaryConfiguration(): Map[String, String] = {
        val propMap: HashMap[String, String] = HashMap()

        HiveConf.ConfVars.values().map(WrappedConfVar(_)).foreach { confvar =>
          if (confvar.varname.contains("datanucleus") || confvar.varname.contains("jdo")) {
            propMap.put(confvar.varname, confvar.getDefaultExpr())
          }
        }
        propMap.put("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$localMetastorePath;create=true")
        propMap.put("datanucleas.rdbms.datastoreAdapterClassName", "org.datanucleus.store.rdbms.adapter.DerbyAdapter")
        propMap.put(ConfVars.METASTOREURIS.varname, "")
        propMap.toMap
      }

      val config = newTemporaryConfiguration()

      SQLContextProvider._sqlContext = new TestHiveContext(sc, config, localMetastorePath, localWarehousePath)
    }
    catch {
      case e: Exception => println(e)
        sys.ShutdownHookThread{
          println("exiting")
        }
    }
    finally {

    }
  }
}


object SQLContextProvider {
  @transient var _sqlContext: HiveContext = _
}