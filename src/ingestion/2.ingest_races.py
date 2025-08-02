# Databricks notebook source
# MAGIC %run "../config"

# COMMAND ----------

from pyspark.sql.functions import col, current_date, current_timestamp, concat, concat_ws, to_timestamp, lit
from pyspark.sql.types import *

# COMMAND ----------

races_schema = StructType([
    StructField("raceId",IntegerType(),False),
    StructField("year",IntegerType(),True),
    StructField("round",IntegerType(), True),
    StructField("circuitId",IntegerType(), True),
    StructField("name",StringType(), True),
    StructField("date",DateType(), True),
    StructField("time",StringType(), True),
    StructField("url",StringType(), True)
])

# COMMAND ----------

df_races = spark.read \
    .format("csv") \
    .option("Header", True) \
    .schema(races_schema) \
    .load(f"dbfs:{raw_folder_path}/races.csv")
display(df_races)

# COMMAND ----------

df_races_raw = df_races \
                    .withColumnRenamed("raceId","race_id") \
                    .withColumnRenamed("year","race_year") \
                    .withColumnRenamed("circuitId","circuit_id") \
                    .withColumn("race_timestamp", to_timestamp(concat("date",lit(" "),"time"), "yyyy-MM-dd HH:mm:ss")) \
                    .withColumn("data_source",lit(v_data_source)) \
                    .drop("url")
df_races_raw = add_ingestion_date(df_races_raw)
display(df_races_raw)

# COMMAND ----------

df_races_raw.write.format("parquet").partitionBy("race_year").mode("overwrite").parquet(f"dbfs:{processed_folder_path}/races")