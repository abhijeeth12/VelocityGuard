from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, count, expr, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "raw_payments"
DB_URL = "jdbc:postgresql://localhost:5432/fraud_db"
DB_USER = "airflow"
DB_PASSWORD = "airflow"
CHECKPOINT_LOCATION = "/tmp/spark_checkpoints_fraud"

# Define schema for the incoming JSON
schema = StructType([
    StructField("tx_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("location", StringType(), True)
])

def create_spark_session():
    # Including PostgreSQL JDBC driver, need to ensure it's available or use --packages
    # We will use org.postgresql:postgresql dependency
    spark = SparkSession.builder \
        .appName("SentinelX_Fraud_Processor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_stream(df, epoch_id):
    # This function writes the micro-batch to PostgreSQL
    # If the table doesn't exist, it creates it
    df.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "processed_payments") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    pass

def main():
    spark = create_spark_session()
    
    # Read from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON
    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Convert timestamp to actual timestamp type
    df_with_ts = parsed_df.withColumn("timestamp", to_timestamp(col("timestamp")))
    
    # Apply watermark
    watermarked_df = df_with_ts.withWatermark("timestamp", "2 minutes")
    
    # Calculate transaction count per user in a 1-minute window using window functions?
    # Actually, structured streaming window operations output a separate DataFrame.
    # To append each individual transaction with a flag, it's slightly tricky without using a Left Semi Join or mapGroupsWithState.
    # We will use foreachBatch to process each microbatch. We can do simple window operations inside the batch.
    
    def process_batch(batch_df, batch_id):
        batch_df.cache()
        if batch_df.count() == 0:
            return
        
        # We can calculate frequency within the batch (or rely on the simpler condition for now to demonstrate)
        # For true stream-stream join it requires a watermark, but inside foreachBatch we process what's available.
        # Let's apply simple rules:
        # Rule 1: amount > 5000
        # Rule 2: user_id has > 3 tx in the current micro-batch (simplified for this exercise)
        
        # Count tx per user
        user_counts = batch_df.groupBy("user_id").count()
        
        # Join back
        joined_df = batch_df.join(user_counts, on="user_id", how="left")
        
        # Apply flags
        flagged_df = joined_df.withColumn(
            "is_fraud",
            when((col("amount") > 5000) | (col("count") > 3), True).otherwise(False)
        ).drop("count")
        
        # Write to Postgres
        flagged_df.write \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", "processed_payments") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
            
        batch_df.unpersist()

    query = df_with_ts.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .trigger(processingTime='10 seconds') \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()
