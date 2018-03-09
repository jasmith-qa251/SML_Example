package com.example.sparktesting

import org.apache.hadoop.hive.conf.HiveConf.ConfVars

private[example] case class WrappedConfVar(cv: ConfVars) {
  val varname = cv.varname
  def getDefaultExpr() = cv.getDefaultExpr()
}