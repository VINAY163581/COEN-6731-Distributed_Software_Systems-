package com.analytics

import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {
  def build(appName: String): SparkSession =
    SparkSession.builder()
      .appName(appName)
      .master("local[*]") 
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
}
