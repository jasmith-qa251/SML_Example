package com.example.sparktesting

import org.apache.spark.{SparkContext, SparkUtils}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait LocalSparkContext extends BeforeAndAfterEach with BeforeAndAfterAll {
  self: Suite =>

  @transient var sc: SparkContext = _

  override def afterEach() {
    resetSparkContext()
    super.afterEach()
  }

  def resetSparkContext() {
    LocalSparkContext.stop(sc)
    sc = null
  }

}

  object LocalSparkContext {
    def stop(sc: SparkContext) {
      Option(sc).foreach {ctx =>
        ctx.stop()
      }
      System.clearProperty("spark.driver.port")
    }

    def withSpark[T](sc: SparkContext)(f: SparkContext => T): T = {
      try {
        f(sc)
      } finally {
        stop(sc)
      }
    }
    def clearLocalRootDirs(): Unit = SparkUtils.clearLocalRootDirs()
  }
