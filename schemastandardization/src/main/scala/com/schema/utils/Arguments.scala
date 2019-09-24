package com.schema.utils

import com.frugalmechanic.optparse.OptParse

class Arguments extends OptParse {

  // Jobs properties
  val inputPath = StrOpt()
  val outputPath = StrOpt()
  // val parquetToCSV = StrOpt()
  val jobType = StrOpt()
  val delimiter = StrOpt()
  val tableName = StrOpt()
  val schemaName = StrOpt()

  // Spark configurations
  val sparkExecutorInstances = IntOpt()
  val sparkExecutorCores = IntOpt()
  val sparkExecutorMemory = StrOpt()

}
