package processor;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

public class Processor {

  // ARGS [DATABASE_HOST]
  public static void main(String[] args) {

    String DBIP = args[0];
    int period = Integer.parseInt(args[1]);
    int percent = Integer.parseInt(args[2]);

    SparkSession spark = SparkSession.builder().appName("Processor Application").getOrCreate();

    Dataset<Row> iucr = readStaticData(spark);
    iucr = iucr.selectExpr("CAST (IUCR as INT)", "`PRIMARY DESCRIPTION`", "`SECONDARY DESCRIPTION`", "`INDEX CODE`");
    // Kafka connection
    Dataset<Row> ds1 = spark.readStream().format("kafka").option("kafka.bootstrap.servers", "cluster-7da8-w-0:9092")
        .option("subscribe", "raw").load();
    Dataset<Row> ds2 = ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

    Dataset<Row> splited = ds2.withColumn("values", functions.split(functions.col("value"), ","));
    Dataset<Row> events = splited.select(functions.col("values").getItem(0).as("id"),
        functions.col("values").getItem(1).as("date"), functions.col("values").getItem(2).as("iucr_number"),
        functions.col("values").getItem(3).as("arrest_str"), functions.col("values").getItem(4).as("domestic_str"),
        functions.col("values").getItem(5).as("district"));

    // Prepare columns
    events = events.withColumn("domestic",
        functions.when(functions.col("domestic_str").equalTo("True"), true).otherwise(false));
    events = events.withColumn("arrest",
        functions.when(functions.col("arrest_str").equalTo("True"), true).otherwise(false));
    events = events.withColumn("timestamp", functions.to_timestamp(functions.col("date")));
    events = events.withColumn("year", functions.year(functions.col("timestamp")));
    events = events.withColumn("month", functions.month(functions.col("timestamp")));
    events = events.selectExpr("CAST (id as INT)", "timestamp", "CAST (year as INT)", "CAST (month as INT)",
        "CAST (iucr_number as INT)", "CAST (arrest as INT)", "CAST (domestic as INT)", "CAST (district as STRING)");

    // Join
    Dataset<Row> crimes = events.join(iucr, functions.col("IUCR").equalTo(functions.col("iucr_number")), "left_outer")
        .select(functions.col("timestamp"), functions.col("year"), functions.col("month"), functions.col("iucr_number"),
            functions.col("arrest"), functions.col("domestic"), functions.col("district"), functions.col("INDEX CODE"),
            functions.col("id"));

    // Aggregation
    Dataset<Row> aggEventData = crimes.withWatermark("timestamp", "1 hour")
        .groupBy(functions.window(functions.col("timestamp"), "1 day"), functions.col("iucr_number"),
            functions.col("year"), functions.col("month"), functions.col("district"))
        .agg(functions.count(functions.col("id")).as("TOTAL"),
            functions.count(functions.when(functions.col("ARREST").equalTo(true), true)).as("ARRESTED"),
            functions.count(functions.when(functions.col("DOMESTIC").equalTo(true), true)).as("DOMESTIC"),
            functions.count(functions.when(functions.col("INDEX CODE").equalTo("I"), true)).as("FBI"));
    
          
    Dataset<Row> aggAnomalyData = crimes.withWatermark("timestamp", "1 hour")
        .groupBy(functions.window(functions.col("timestamp"), Integer.toString(period) + " days", "1 day"),
            functions.col("district"))
        .agg(functions.count(functions.col("id")).as("TOTAL"),
            functions.count(functions.when(functions.col("INDEX CODE").equalTo("I"), true)).as("FBI"));

    aggAnomalyData = aggAnomalyData.withColumn("Percent", functions.expr("(FBI / (TOTAL)) * 100"));
    aggAnomalyData = aggAnomalyData.withColumn("Start", functions.to_date(functions.col("window.start")));
    aggAnomalyData = aggAnomalyData.withColumn("End", functions.to_date(functions.col("window.end")));

    // StreamingQuery query =
    // aggData.writeStream().outputMode("append").format("console").option("truncate",
    // "false")
    // .option("numRows", 100).start();

    ForeachEventWriter eventWriter = new ForeachEventWriter();
    eventWriter.option("dbhost", args[0]);
    eventWriter.option("dbpassword", "qazxcvbnm");
    eventWriter.option("dbuser", "postgres");
    eventWriter.option("dbport", "5432");
    eventWriter.option("dbtable", "crimes");

    StreamingQuery eventQuery = aggEventData
        .selectExpr("year", "month", "district", "CAST (iucr_number as STRING)", "CAST (TOTAL as INT)",
            "CAST (DOMESTIC as INT)", "CAST (ARRESTED as INT)", "CAST (FBI as INT)")
        .writeStream().outputMode("append").foreach(eventWriter).start();

    

    ForeachAnomalyWriter anomalyWriter = new ForeachAnomalyWriter();
    anomalyWriter.option("dbhost", DBIP);
    anomalyWriter.option("dbpassword", "qazxcvbnm");
    anomalyWriter.option("dbuser", "postgres");
    anomalyWriter.option("dbport", "5432");
    anomalyWriter.option("dbtable", "anomalies");

    StreamingQuery anomalyQuery = aggAnomalyData
        .selectExpr("CAST(start AS STRING)", "CAST(end AS STRING)", "district", "CAST (TOTAL as INT)",
            "CAST (FBI as INT)", "CAST (Percent as FLOAT)")
        .filter("Percent > " + Integer.toString(percent)).writeStream().outputMode("append").foreach(anomalyWriter).start();

    try {
      System.out.println("Ready -- ");
      eventQuery.awaitTermination();
      anomalyQuery.awaitTermination();
    } catch (org.apache.spark.sql.streaming.StreamingQueryException e) {
      System.out.println("Something is wrong");
      e.printStackTrace();
    }

    spark.stop();
  }

  private static Dataset<Row> readStaticData(SparkSession spark) {
    String iucrLocation = "gs:/crimes/Chicago_Police_Department_-_Illinois_Uniform_Crime_Reporting__IUCR__Codes.csv";
    Dataset<Row> iucr = spark.read().option("header", "true").csv(iucrLocation);
    return iucr;
  }
}