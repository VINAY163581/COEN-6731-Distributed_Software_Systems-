package com.analytics

import com.analytics.data.DataLoader
import com.analytics.session.Sessionization
import com.analytics.funnel.FunnelAnalysis
import com.analytics.attribution.Attribution
import com.analytics.anomaly.AnomalyDetection
import com.analytics.metrics.Metrics
import org.apache.spark.sql.SparkSession

object MainApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Ecommerce Analytics")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Use DATA_PATH if provided (recommended in Dataproc submit),
    // otherwise fall back to your real bucket path.
    val basePath = sys.env.getOrElse("DATA_PATH", "gs://vinay-ecommerce-dataproc-6731/data")

    println(s"Using DATA_PATH = $basePath")
    println(s"Loading files:")
    println(s"  events  -> $basePath/events.csv")
    println(s"  orders  -> $basePath/orders.csv")
    println(s"  catalog -> $basePath/catalog.csv")

    val events  = DataLoader.loadEvents(spark, s"$basePath/events.csv")
    val orders  = DataLoader.loadOrders(spark, s"$basePath/orders.csv")
    val catalog = DataLoader.loadCatalog(spark, s"$basePath/catalog.csv")

    val sessions = Sessionization.buildSessions(events)

    val funnel = FunnelAnalysis.computeFunnel(sessions, catalog)

    val lookbackHours = 24
    val attribution = Attribution.lastTouchAttribution(
      sessions,
      orders,
      lookbackHours
    )

    val anomalies = AnomalyDetection.detectAnomalies(funnel)

    println("========= METRICS =========")
    println(s"Total Events: ${Metrics.totalEvents(events)}")
    println(s"Total Orders: ${Metrics.totalOrders(orders)}")
    println(s"Total Revenue: ${Metrics.totalRevenue(orders)}")
    println(s"Average Order Value: ${Metrics.averageOrderValue(orders)}")
    println(s"Conversion Rate: ${Metrics.conversionRate(events, orders)}")

    println("\nEvents by Type:")
    Metrics.eventsByType(events).show(false)

    println("\nTop Selling Products:")
    Metrics.topSellingProducts(orders, catalog).show(false)

    println("\n========= FUNNEL =========")
    funnel.show(false)

    println("\n========= ATTRIBUTION =========")
    attribution.show(false)

    println("\n========= ANOMALIES =========")
    anomalies.show(false)

    spark.stop()
  }
}
