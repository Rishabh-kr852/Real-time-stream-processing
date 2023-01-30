from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

from logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("File Streaming") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

    logger = Log4j(spark)

    raw_df = spark.readStream \
        .format("json") \
        .option("path", "input") \
        .load()

    # raw_df.printSchema()
    explode_df = raw_df.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID", "CustomerType", "PaymentMethod",
                                   "DeliveryType", "DeliveryAddress.City", "DeliveryAddress.State","DeliveryAddress.PinCode",
                                   "explode(InvoiceLineItems) as LineItem")

    # explode_df.printSchema()
    flatten_df = explode_df \
        .withColumn("ItemCode", expr("LineItem.ItemCode")) \
        .withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
        .withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
        .withColumn("ItemQty", expr("LineItem.ItemQty")) \
        .withColumn("TotalValue", expr("LineItem.TotalValue")) \
        .drop("LineItem")

    invoice_writer_query = flatten_df.writeStream \
        .format("json") \
        .option("path", "output") \
        .option("checkPointLocation", "chk-point-dir") \
        .outputMode("append") \
        .queryName("Flattened invoice writer") \
        .trigger(processingTime="1 minute") \
        .start()

    logger.info("Flattened invoice writer started")
    invoice_writer_query.awaitTermination()
