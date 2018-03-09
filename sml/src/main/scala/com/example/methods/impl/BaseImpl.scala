package com.example.methods.impl

import org.apache.spark.sql.DataFrame

import scala.util.Try

object BaseImpl {

  implicit class BaseMethodsImpl(df: DataFrame) {

    /**This function will check the names of columns in the dataframe against a list of inputs to check that any
      * methods that require specific columns will not fail.
      * @author james.a.smith@ext.ons.gov.uk
      * @version 1.0
      * @param columns String* - Name of the new Column
      * @return DataFrame
      */
    def checkColNames(columns: String*): DataFrame = {

      val colsFound = columns.flatMap(col => Try(df(col)).toOption)

      val okToContinue = columns.size == colsFound.size

      if (!okToContinue) throw ONSRuntimeException("Missing Columns Detected") else df
    }
  }
}
