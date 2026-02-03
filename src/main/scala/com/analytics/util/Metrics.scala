package com.analytics.metrics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders

object Metrics {

  
  def totalEvents(events: DataFrame): Long = events.count()

  
  def totalOrders(orders: DataFrame): Long = orders.count()

  
  def totalRevenue(orders: DataFrame): Double = 
    orders.agg(sum("total_amount")).as[Double](Encoders.scalaDouble).first()

  
  def averageOrderValue(orders: DataFrame): Double = 
    totalRevenue(orders) / totalOrders(orders)

  
  def conversionRate(events: DataFrame, orders: DataFrame): Double = 
    totalOrders(orders).toDouble / totalEvents(events)

  
  def eventsByType(events: DataFrame): DataFrame = 
    events.groupBy("event_type").count()

  
  def topSellingProducts(orders: DataFrame, catalog: DataFrame): DataFrame = {
 
    val ordersWithProduct = orders.withColumn("product_id", lit(1)) 
    val catalogWithProduct = catalog.withColumn("product_id", col("product_id"))

    ordersWithProduct
      .groupBy("product_id")
      .count()
      .join(catalogWithProduct, Seq("product_id"), "left")
      .select(
        col("product_id"),
        col("brand").alias("product_name"), 
        col("category"),
        col("price"),
        col("count").alias("sold_count")
      )
      .orderBy(col("sold_count").desc)
  }
}
