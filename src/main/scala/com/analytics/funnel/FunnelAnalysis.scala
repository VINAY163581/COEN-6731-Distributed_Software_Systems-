package com.analytics.funnel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FunnelAnalysis {

  def computeFunnel(events: DataFrame, catalog: DataFrame): DataFrame = {

    val enriched = events
      .join(catalog, "product_id")
      .withColumn("event_date", to_date(col("timestamp")))

    val pivoted = enriched
      .groupBy("category", "device", "referrer", "event_date", "derived_session_id")
      .pivot("event_type", Seq("view", "add_to_cart", "purchase"))
      .count()
      .na.fill(0)

    pivoted
      .groupBy("category", "device", "referrer", "event_date")
      .agg(
        sum("view").as("views"),
        sum("add_to_cart").as("adds"),
        sum("purchase").as("purchases")
      )
      .withColumn("view_to_add_rate", col("adds") / col("views"))
      .withColumn("add_to_purchase_rate", col("purchases") / col("adds"))
  }
}
