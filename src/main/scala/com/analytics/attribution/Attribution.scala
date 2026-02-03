package com.analytics.attribution

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Attribution {

  def lastTouchAttribution(events: DataFrame, orders: DataFrame, kHours: Int): DataFrame = {

   
    val e = events.alias("e")
    val o = orders.alias("o")

    // Join on user_id
    val joined = o.join(e, col("o.user_id") === col("e.user_id"))
      .where(
        col("e.event_type") =!= "direct" &&
        col("e.timestamp") <= col("o.timestamp") &&
        col("e.timestamp") >= col("o.timestamp") - expr(s"INTERVAL $kHours HOURS")
      )

    
    val w = Window.partitionBy(col("o.order_id")).orderBy(col("e.timestamp").desc)

    joined
      .withColumn("rank", row_number().over(w))
      .filter(col("rank") === 1)
      .select(
        col("o.order_id"),
        col("o.user_id"),
        col("e.referrer"),
        col("o.total_amount")
      )
  }
}
