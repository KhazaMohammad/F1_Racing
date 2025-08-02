# Databricks notebook source
# MAGIC %run "../config"

# COMMAND ----------

from pyspark.sql.functions import col, current_date, current_timestamp, concat, concat_ws, to_timestamp, lit, explode
from pyspark.sql.types import *

# COMMAND ----------

qualify_schema = StructType([
    StructField("constructorId",IntegerType(),True),
    StructField("driverId",IntegerType(),True),
    StructField("number",IntegerType(), True),
    StructField("position",IntegerType(), True),
    StructField("q1",StringType(), True),
    StructField("q2",StringType(), True),
    StructField("q3",StringType(), True),
    StructField("qualifyId",IntegerType(), True),
    StructField("raceId",IntegerType(), True)
])

# COMMAND ----------

df_qualify = spark.read \
    .format("json") \
    .schema(qualify_schema) \
    .option("multiline", True) \
    .load(f"dbfs:{raw_folder_path}/qualifying/qualifying_split_*.json")
display(df_qualify)

# COMMAND ----------

df_qualify_raw = df_qualify \
                    .select(col("qualifyId").alias("qualifying_id"),
                            col("raceId").alias("race_id"),
                            col("driverId").alias("driver_id"),
                            col("constructorId").alias("constructor_id"),
                            col("number"),
                            col("position"),
                            col("q1"),col("q2"),col("q3")
                            ) \
                    .withColumn("data_source",lit(v_data_source)) \
                    .withColumn("Ingestion_date", current_timestamp())
df_qualify_raw = add_ingestion_date(df_qualify_raw)
display(df_qualify_raw)

# COMMAND ----------

df_qualify_raw.write.format("parquet").mode("overwrite").parquet("dbfs:/mnt/processed_databricksstoragef1/qualifying")