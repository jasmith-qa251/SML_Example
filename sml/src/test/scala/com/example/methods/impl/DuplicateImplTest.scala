package com.example.methods.impl

import com.example.methods.impl.DuplicateImpl._
import com.example.sparktesting.TestSparkContext
import org.apache.spark.sql.DataFrame

class DuplicateImplTest extends TestSparkContext {

  test("test duplicateMarking implicit") {
    // Input data
    val inData: String = "./src/test/resources/inputs/DuplicateMarker.json"
    val inDf: DataFrame = _hc.read.json(inData).select("id", "num", "order")
    println("Input dataframe")
    inDf.show()

    // Expected data
    val expData: String = "./src/test/resources/outputs/DuplicateMarker.json"
    val expDf: DataFrame = _hc.read.json(expData).select("id", "num", "order", "marker")
    println("Expected dataframe")
    expDf.show()

    // Create actual output
    val outDf: DataFrame = inDf.duplicateMarking(List("id", "num"), List("order"), "marker")
    println("Output dataframe")
    outDf.show()

    // Assert
    assertResult(expDf.collect()) {
      outDf.collect()
    }
  }
}
