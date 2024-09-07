import json
from pyspark.sql.functions import col, explode, split, struct, array, lit, udf, from_json, window, to_date, to_timestamp,current_date, to_timestamp, max, avg, round, count,row_number, when
from pyspark.sql.types import StringType, IntegerType, TimestampType, StructType, StructField, ArrayType, DateType, BooleanType, BinaryType, LongType, NullType, DoubleType, MapType

spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true") 
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.fileSize", "134217728")
# spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "true")
spark.conf.set("spark.sql.streaming.stateStore.stateSchemaCheck", "false")

confluentClusterName = ""
confluentBootstrapServers = ""
confluentTopicName = "station_information"
schemaRegistryUrl = ""
confluentApiKey = dbutils.secrets.get(scope = "take_home_assignment", key = "ConfluentClusterAPIKey")
confluentSecret = dbutils.secrets.get(scope = "take_home_assignment", key = "ConfluentClusterAPISecret")
# schemaRegistryApiKey = dbutils.secrets.get(scope = "take_home_assignment", key = "ConfluentSchemaRegistryKey")
# schemaRegistrySecret = dbutils.secrets.get(scope = "take_home_assignment", key = "ConfluentSchemaRegistrySecret")
deltaTablePath_test = "dbfs:/delta/landing/table/station_information_landing_test"
checkpointPath_test = "dbfs:/delta/landing/checkpoints/station_information_test"
checkPointPath_stream = "dbfs:/delta/stream/checkpoints/station_information_stream"

schema = StructType([
    StructField("data", StructType([
        StructField("stations", ArrayType(StructType([
                                    StructField("short_name", StringType(), True),
                                    StructField("rental_methods", ArrayType(StringType(), True)),
                                    StructField("has_kiosk", BooleanType(), True),
                                    StructField("capacity", IntegerType(), True),
                                    StructField("station_type", StringType(), True),
                                    StructField("lon", DoubleType(), True),
                                    StructField("electric_bike_surcharge_waiver", BooleanType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("region_id", StringType(), True),
                                    StructField("eightd_has_key_dispenser", BooleanType(), True),
                                    StructField("eightd_station_services", ArrayType(StringType(), True)),
                                    StructField("lat", DoubleType(), True),
                                    StructField("station_id", StringType(), True),
                                    StructField("rental_uris", MapType(StringType(),StringType()), True),
                                    StructField("external_id", StringType(), True)
                                         ])
                            ), True)])),
    StructField("last_updated", LongType(), True),
    StructField("ttl", IntegerType(), True),
    StructField("version", StringType(), True)
])

import pyspark.sql.functions as fn
from pyspark.sql.types import StringType

kafka_df = (
  spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", confluentBootstrapServers)
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret))
  .option("kafka.ssl.endpoint.identification.algorithm", "https")
  .option("kafka.sasl.mechanism", "PLAIN")
  .option("subscribe", confluentTopicName)
  .option("startingOffsets", "latest")
  .option("failOnDataLoss", "false")
  .load()
  .selectExpr("CAST(value AS STRING) as value", "topic", "partition", "offset", "timestamp")
  .select(from_json(col("value"),schema).alias("parsedValue"), col("topic"), col("partition"), col("offset"), col("timestamp")) 
  .withWatermark("timestamp", "30 seconds")     
)

# Explode the stations array into separate rows
exploded_df = kafka_df \
    .withColumn("batch_run_date", to_date(current_date(), "yyyy-MM-dd")) \
    .withColumn("station", explode(col("parsedValue.data.stations")))

# Select individual fields from the exploded rows
agg_df = exploded_df \
    .select(
        col("batch_run_date"),
        col("station.station_id").alias("station_id"),
        col("station.lon").alias("longitude"),
        col("station.lat").alias("latitude"),
        col("station.name").alias("name"),
        col("station.region_id").alias("region_id"),
        col("station.capacity").alias("capacity"),
        col("timestamp"),
        col("topic"),
        col("parsedValue.last_updated")
    ) \
    .groupBy(
        window("timestamp", "10 minutes"),
        "station_id", "longitude", "latitude", "name", "region_id", "capacity", "topic", "batch_run_date"
    ) \
    .agg(
        max("last_updated").alias("max_last_updated")
    ) \
    .select(
        col("window.start").alias("window_start"),
        "station_id",
        "longitude",
        "latitude",
        "name",
        when(col("region_id").isNull(), "-1").otherwise(col("region_id")).alias("region_id"),
        "capacity",
        to_timestamp("max_last_updated").alias("last_updated_tmp"),
        "batch_run_date"
    )


from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

private_key_obj = open("","r")
private_key=private_key_obj.read()
private_key_obj.close()

key = bytes(private_key, 'utf-8')

p_key = serialization.load_pem_private_key(key, password="".encode(), backend=default_backend())

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
).replace(b"-----BEGIN PRIVATE KEY-----\n", b"") \
    .replace(b"\n-----END PRIVATE KEY-----", b"") \
        .decode("utf-8")

# COMMAND ----------

# Use dbutils secrets to get Snowflake credentials.
user = dbutils.secrets.get("snowflake-data-warehouse", "snowflakeUsername")
password = dbutils.secrets.get("snowflake-data-warehouse", "snowflakePassword")
 
sfOptions = {
  "sfUrl": "",
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "CITIBIKE",
  "sfSchema": "RAW",
  "sfWarehouse": "WH",
  "pem_private_key": pkb, 
  "sfRole": "accountadmin"
}

def foreach_batch_function(df, batch_id):
    df.write\
      .format("snowflake")\
      .mode("append")\
      .options(**sfOptions)\
      .option("streaming_stage", "CITIBIKE.RAW.BIKE_STATION_INFORMATION_STAGE")\
      .save()

streaming_query = agg_df.writeStream \
                        .format("snowflake")\
                        .outputMode("append") \
                        .trigger(processingTime="5 seconds")\
                        .option("checkpointLocation", checkPointPath_stream)\
                        .foreachBatch(foreach_batch_function) \
                        .start()
  
streaming_query.awaitTermination()
