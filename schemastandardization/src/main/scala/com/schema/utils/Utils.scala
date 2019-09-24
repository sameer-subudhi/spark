package com.schema.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types._

object Utils {

  val fs = FileSystem.get(new Configuration())

  def deleteOutput(outputPath: String): Unit = {
    if (fs.exists(new Path(outputPath))) {
      fs.delete(new Path(outputPath), true)
    }
  }

  def toLowerCamelCase(x: String): String = {
    val parts = x.split("_")
    parts.head + parts.tail.map(e => {
      e.head.toUpper + e.tail
    }).mkString
  }

  def getDataType(dtype: String): DataType =
    dtype match {
      case "bit" | "integer" | "inet" | "smallint" => IntegerType
      case "bigint" => LongType
      case "real" | "numeric" | "double precision" => DoubleType
      case "character" => StringType
      case "boolean" => BooleanType
      case _ => StringType
    }
}