package com.example.api.java.methods

import java.util

import com.example.methods.Duplicate
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._

class JavaDuplicate[K](dm: Duplicate) {
  // TODO find out how to implement default values
  def dm1(df: DataFrame, partCol: util.ArrayList[String], ordCol: util.ArrayList[String],
          new_col: String): DataFrame = {
            dm.dm1(df, partCol.toList, ordCol.toList, new_col)
  }
}
object JavaDuplicate{

  def duplicate(df: DataFrame) : JavaDuplicate[Duplicate] = {
    new JavaDuplicate(Duplicate.duplicate(df))}
}