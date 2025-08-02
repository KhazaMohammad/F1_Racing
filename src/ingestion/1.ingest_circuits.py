# Databricks notebook source
# MAGIC %run "../config"

# COMMAND ----------

from pyspark.sql.functions import col, current_date, current_timestamp, lit
from pyspark.sql.types import *

# COMMAND ----------

circuits_schema = StructType([
    StructField("circuitId",IntegerType(),False),
    StructField("circuitRef",StringType(),True),
    StructField("name",StringType(), True),
    StructField("location",StringType(), True),
    StructField("country",StringType(), True),
    StructField("lat",DoubleType(), True),
    StructField("lng",DoubleType(), True),
    StructField("alt",IntegerType(), True),
    StructField("url",StringType(), True)
]
)

# COMMAND ----------

df_circuits = spark.read \
    .format("csv") \
    .option("Header", True) \
    .schema(circuits_schema) \
    .load(f"dbfs:{raw_folder_path}/circuits.csv")
display(df_circuits)

# COMMAND ----------

df_circuits_raw = df_circuits \
                    .withColumnRenamed("circuitId","circuit_id") \
                    .withColumnRenamed("circuitRef","circuit_ref") \
                    .withColumnRenamed("lat","latitude") \
                    .withColumnRenamed("lng","longitude") \
                    .withColumnRenamed("alt","altitude") \
                    .withColumnRenamed("alt","altitude") \
                    .withColumn("data_source",lit(v_data_source)) \
                    .drop("url")

df_circuits_raw = add_ingestion_date(df_circuits_raw)
display(df_circuits_raw)

# COMMAND ----------

print("Ingesting the data for 1.Circuits file")

# COMMAND ----------

df_circuits_raw.write.format("parquet").mode("overwrite").parquet(f"{processed_folder_path}/circuits")