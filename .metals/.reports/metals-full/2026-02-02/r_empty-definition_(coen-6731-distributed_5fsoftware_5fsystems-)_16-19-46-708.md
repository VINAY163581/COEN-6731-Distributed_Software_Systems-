error id: file://<WORKSPACE>/src/main/scala/com/analytics/MainApp.scala:com/analytics/SparkSessionBuilder.
file://<WORKSPACE>/src/main/scala/com/analytics/MainApp.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb

found definition using fallback; symbol SparkSessionBuilder
offset: 427
uri: file://<WORKSPACE>/src/main/scala/com/analytics/MainApp.scala
text:
```scala
package com.analytics

import com.analytics.data.DataLoader
import com.analytics.session.Sessionization
import com.analytics.funnel.FunnelAnalysis
import com.analytics.attribution.Attribution
import com.analytics.anomaly.AnomalyDetection
import com.analytics.metrics.Metrics          // ✅ NEW
import org.apache.spark.sql.SparkSession



object MainApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSessionB@@uilder.build("Ecommerce Analytics")
    spark.sparkContext.setLogLevel("WARN")

    // ================= LOAD DATA =================
    val events  = DataLoader.loadEvents(spark, "data/events.csv")
    val orders  = DataLoader.loadOrders(spark, "data/orders.csv")
    val catalog = DataLoader.loadCatalog(spark, "data/catalog.csv")

    // ================= CORE PIPELINE =================
    val sessions = Sessionization.buildSessions(events)

    val funnel = FunnelAnalysis.computeFunnel(sessions, catalog)

    val lookbackHours = 24 // Define lookbackHours explicitly
    val attribution = Attribution.lastTouchAttribution(
      sessions,
      orders,
      lookbackHours
    )

    val anomalies = AnomalyDetection.detectAnomalies(funnel)

    // ================= METRICS =================
    println("========= METRICS =========")
    println(s"Total Events: ${Metrics.totalEvents(events)}")
    println(s"Total Orders: ${Metrics.totalOrders(orders)}")
    println(s"Total Revenue: ${Metrics.totalRevenue(orders)}")
    println(s"Average Order Value: ${Metrics.averageOrderValue(orders)}")
    println(s"Conversion Rate: ${Metrics.conversionRate(events)}")

    println("\nEvents by Type:")
    Metrics.eventsByType(events).show(false)

    println("\nTop Selling Products:")
    Metrics.topSellingProducts(orders, catalog).show(false)

    // ================= OUTPUT =================
    println("\n========= FUNNEL =========")
    funnel.show(false)

    println("\n========= ATTRIBUTION =========")
    attribution.show(false)

    println("\n========= ANOMALIES =========")
    anomalies.show(false)

    spark.stop()
  }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 