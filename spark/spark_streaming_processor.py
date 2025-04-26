from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType
import json
from db.db_writer import insert_aggregate, insert_alert

schema = StructType() \
    .add("user_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("heart_rate", IntegerType()) \
    .add("steps", IntegerType()) \
    .add("calories_burned", DoubleType())

spark = SparkSession.builder \
    .appName("FitnessTrackerStreaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "fitness_raw") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .selectExpr(
        "data.user_id",
        "CAST(data.timestamp AS TIMESTAMP) as timestamp",
        "data.heart_rate",
        "data.steps",
        "data.calories_burned"
    )

agg_df = df_parsed.groupBy(
    col("user_id"),
    window(col("timestamp"), "15 minutes")
).agg(
    {"heart_rate": "avg", "steps": "sum", "calories_burned": "sum"}
).withColumnRenamed("avg(heart_rate)", "avg_heart_rate") \
 .withColumnRenamed("sum(steps)", "total_steps") \
 .withColumnRenamed("sum(calories_burned)", "total_calories")

alerts_df = df_parsed.filter(col("heart_rate") > 160)

def write_aggregates_to_mysql(batch_df, batch_id):
    records = batch_df.collect()
    for row in records:
        json_data = json.loads(row["value"])
        insert_aggregate(json_data)

def write_alerts_to_mysql(batch_df, batch_id):
    records = batch_df.collect()
    for row in records:
        json_data = json.loads(row["value"])
        insert_alert(json_data)

agg_output = agg_df.selectExpr("to_json(struct(*)) AS value")
alerts_output = alerts_df.selectExpr("to_json(struct(*)) AS value")

# Write user_aggregates to MySQL
agg_query = agg_output.writeStream \
    .foreachBatch(write_aggregates_to_mysql) \
    .outputMode("update") \
    .start()

# Write alerts to MySQL
alerts_query = alerts_output.writeStream \
    .foreachBatch(write_alerts_to_mysql) \
    .outputMode("append") \
    .start()

# Write user_aggregates to Kafka topic
agg_kafka_query = agg_output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "user_aggregates") \
    .option("checkpointLocation", "/tmp/checkpoints/agg_kafka") \
    .outputMode("update") \
    .start()

# Write alerts to Kafka topic
alerts_kafka_query = alerts_output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "alerts") \
    .option("checkpointLocation", "/tmp/checkpoints/alerts_kafka") \
    .outputMode("append") \
    .start()


agg_query.awaitTermination()
alerts_query.awaitTermination()
agg_kafka_query.awaitTermination()
alerts_kafka_query.awaitTermination()


