package com.schema.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkConfiguration {

  val cmdArgs = new Arguments

  def configureSparkSession(): SparkSession = {
    val conf = new SparkConf()

    if (cmdArgs.sparkExecutorInstances) {
      conf.set("spark.executor.instances",  "100") // cmdArgs.sparkExecutorInstances.get.toString)
    }
    if (cmdArgs.sparkExecutorCores) {
      conf.set("spark.executor.cores", "8") // cmdArgs.sparkExecutorCores.get.toString)
    }
    if (cmdArgs.sparkExecutorMemory) {
      conf.set("spark.executor.memory", "29g")  // cmdArgs.sparkExecutorMemory.get)
    }

    SparkSession.builder().config(conf).getOrCreate()
  }
}
