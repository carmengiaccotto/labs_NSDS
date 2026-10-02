package it.polimi.nsds.eval;

/*
 * Members:
 * - Carmen Giaccotto
 * - Alessia Franchetti-Rosada
 * - Alessandro Paolo Gianni Callegari
 */

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import static org.apache.spark.sql.functions.*;

public class ProductAnalytics {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .master("local[8]")
                .appName("ProductAnalytics")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Load static datasets
        Dataset<Row> products = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("input/products.csv");
        // ProductID, Category, Price

        Dataset<Row> historicalPurchases = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("input/historicalPurchases.csv");
        // ProductID, Quantity

        // Streaming source: purchasesStream
        Dataset<Row> purchasesStream = spark.readStream()
                .format("rate")
                .option("rowsPerSecond", 5)
                .load();

        // Map the "value" counter to product IDs and quantities
        purchasesStream = purchasesStream
                .select(
                        col("timestamp"),
                        when(col("value").mod(10).lt(2), lit("P0582"))
                                .when(col("value").mod(10).lt(3), lit("P0112"))
                                .when(col("value").mod(10).lt(5), lit("P0536"))
                                .when(col("value").mod(10).equalTo(6), lit("P0742"))
                                .when(col("value").mod(10).equalTo(8), lit("P0027"))
                                .otherwise(lit("P00" + Math.random() % 100))
                                .alias("Product"),
                        (col("value").mod(5).plus(1)).alias("Quantity")       // 1–5 units
                );

        products.cache();
        historicalPurchases.cache();

        // ==========================================
        // Q1: Top 5 most purchased products
        // ==========================================

        Dataset<Row> topProducts = historicalPurchases
                .withColumnRenamed("Quantity", "totalQuantity")
                .orderBy(desc("totalQuantity"))
                .limit(5);

        topProducts.cache();
        topProducts.show();

        // ==========================================
        // Q2: Revenue per (Category, TopProduct)
        // ==========================================

        Dataset<Row> revenue = topProducts
                .join(products, "ProductID")
                .withColumn("totalRevenue", col("Price").multiply(col("totalQuantity")))
                .select("Category", "ProductID", "totalRevenue");

        revenue.show();

        // ==========================================
        // Q3: Streaming window for top products
        // ==========================================

        Dataset<Row> windowed = purchasesStream
                .join(topProducts, col("Product").equalTo(col("ProductID")))
                .groupBy(
                        window(col("timestamp"), "15 seconds", "5 seconds"),
                        col("Product")
                )
                .sum("Quantity")
                .withColumnRenamed("sum(Quantity)", "streamQuantity");

        StreamingQuery q3 = windowed
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        // ==========================================
        // Q4: Difference historical vs streaming
        // ==========================================

        Dataset<Row> diff = windowed
                .join(topProducts, col("Product").equalTo(col("ProductID")))
                .withColumn("difference", col("totalQuantity").minus(col("streamQuantity")))
                .drop("ProductID");

        StreamingQuery q4 = diff
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        try {
            q3.awaitTermination();
            q4.awaitTermination();
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }

        spark.close();
    }
}
