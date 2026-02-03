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
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    
    val events  = DataLoader.loadEvents(spark, "data/events.csv")
    val orders  = DataLoader.loadOrders(spark, "data/orders.csv")
    val catalog = DataLoader.loadCatalog(spark, "data/catalog.csv")

   
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
    println(s"Conversion Rate: ${Metrics.conversionRate(events, orders)}") // ✅ FIXED

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
