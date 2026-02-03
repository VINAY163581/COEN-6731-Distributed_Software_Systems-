package com.analytics.anomaly
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object AnomalyDetection {

  def detectAnomalies(funnel: DataFrame): DataFrame = {

    val w = Window
      .partitionBy("category", "device", "referrer")
      .orderBy("event_date")
      .rowsBetween(-7, -1)

    funnel
      .withColumn("avg_7d", avg("add_to_purchase_rate").over(w))
      .withColumn("delta", col("add_to_purchase_rate") - col("avg_7d"))
      .withColumn(
        "anomaly",
        abs(col("delta")) > col("avg_7d") * 0.3
      )
      .filter(col("anomaly"))
  }
}
