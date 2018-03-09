package com.example.methods

import com.example.methods.impl.DuplicateImpl
import org.apache.spark.sql.DataFrame

class Duplicate (val dfIn: DataFrame) extends BaseMethod {

  if (dfIn == null) throw new Exception("DataFrame cannot be null")

  val defaultCol = "DuplicateMarking"

  import DuplicateImpl._

  // TODO Replace the below checks with Options

  def dm1(df: DataFrame, partCol: List[String], ordCol: List[String], new_col: String = defaultCol) : DataFrame = {

    mandatoryArgCheck(partCol, ordCol, new_col)

    val dF = if (df == null) dfIn else df

    val cols: List[String] = partCol ++ ordCol

    dF.checkColNames(cols: _*)
      .duplicateMarking(partCol, ordCol, new_col)
  }
}

object Duplicate {

  def duplicate(df: DataFrame) : Duplicate = new Duplicate(df)
}
