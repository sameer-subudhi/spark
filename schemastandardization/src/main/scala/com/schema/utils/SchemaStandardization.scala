package com.schema.utils

import Utils._
import org.apache.spark.sql.types.{StructField, StructType}

object SchemaStandardization {

  def main(args: Array[String]): Unit = {

    val cmdArgs = new Arguments
    cmdArgs.parse(args)

    val spark = SparkConfiguration.configureSparkSession

    cmdArgs.jobType.get match {
      case JobTypes.PARQUET_TO_CSV =>
        val data = spark.read.parquet(cmdArgs.inputPath.get)
        deleteOutput(cmdArgs.outputPath.get)
        data.write.option("delimiter", "\t") //cmdArgs.delimiter.get
          .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
          .csv(cmdArgs.outputPath.get)

      case JobTypes.PARTQUET_TO_PARTQUET =>
        val data = spark.read.parquet(cmdArgs.inputPath.get)
        val colNames = data.schema.fieldNames.map(toLowerCamelCase)
        deleteOutput(cmdArgs.outputPath.get)
        data.toDF(colNames: _*).write.option("compression", "gzip").parquet(cmdArgs.outputPath.get)

      case JobTypes.CSV_TO_PARQUET =>
        val sm = new SchemaModel
        val struct = sm.getSchemaFromDB(cmdArgs.schemaName.get, cmdArgs.tableName.get)
        val schema = StructType(struct.map(x => StructField(x._1, getDataType(x._2), true)))
        val data = spark.read.option("delimiter", cmdArgs.delimiter.get)
          .schema(schema).csv(cmdArgs.inputPath.get)
        deleteOutput(cmdArgs.outputPath.get)
        data.write.option("compression", "gzip").parquet(cmdArgs.outputPath.get)

    }
  }

}