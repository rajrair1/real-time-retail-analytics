import os
from pyspark.sql import SparkSession, functions as F, types as T

schema = T.StructType([
    T.StructField("event_id", T.StringType(), False),
    T.StructField("event_time", T.TimestampType(), False),
    T.StructField("customer_id", T.StringType()),
    T.StructField("product_id", T.StringType(), False),
    T.StructField("store_id", T.StringType(), False),
    T.StructField("quantity", T.IntegerType(), False),
    T.StructField("unit_price", T.DecimalType(12, 2), False),
    T.StructField("discount_rate", T.DecimalType(5, 4), False),
    T.StructField("payment_method", T.StringType()),
])

spark = SparkSession.builder.appName("retail-analytics").getOrCreate()
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
       .option("subscribe", os.getenv("KAFKA_TOPIC", "retail_transactions"))
       .option("startingOffsets", "earliest").load())

parsed = raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e")).select("e.*")
valid = (parsed.filter(
    F.col("event_id").isNotNull() & (F.col("quantity") > 0) &
    (F.col("unit_price") >= 0) & F.col("discount_rate").between(0, 1))
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["event_id"])
    .withColumn("net_amount", F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_rate")), 2))
    .withColumn("date_key", F.date_format("event_time", "yyyyMMdd").cast("int")))

jdbc_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:5432/{os.getenv('POSTGRES_DB', 'retail_analytics')}"

def write_batch(frame, _batch_id):
    (frame.write.format("jdbc").mode("append").option("url", jdbc_url)
     .option("dbtable", "fact_sales").option("user", os.getenv("POSTGRES_USER", "retail"))
     .option("password", os.getenv("POSTGRES_PASSWORD", "retail")).save())

(valid.writeStream.foreachBatch(write_batch)
 .option("checkpointLocation", "/tmp/checkpoints/retail")
 .trigger(processingTime="30 seconds").start().awaitTermination())
