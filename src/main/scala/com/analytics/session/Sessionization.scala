package com.analytics.session
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Sessionization {

  def buildSessions(events: DataFrame, timeoutMinutes: Int = 30): DataFrame = {

    val w = Window.partitionBy("user_id").orderBy("timestamp")

    val withLag = events
      .withColumn("prev_ts", lag("timestamp", 1).over(w))
      .withColumn(
        "gap_minutes",
        (unix_timestamp(col("timestamp")) - unix_timestamp(col("prev_ts"))) / 60
      )
      .withColumn(
        "new_session",
        when(col("gap_minutes").isNull || col("gap_minutes") > timeoutMinutes, 1).otherwise(0)
      )

    withLag
      .withColumn(
        "derived_session_id",
        sum("new_session").over(w)
      )
      .drop("prev_ts", "gap_minutes", "new_session")
  }
}
