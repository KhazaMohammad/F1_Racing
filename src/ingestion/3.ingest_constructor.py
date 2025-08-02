# Databricks notebook source
# MAGIC %run "../config"

# COMMAND ----------

from pyspark.sql.functions import col, current_date, current_timestamp, concat, concat_ws, to_timestamp, lit
from pyspark.sql.types import *

# COMMAND ----------

constructors_schema = StructType([
    StructField("constructorId",DoubleType(),False),
    StructField("constructorRef",StringType(),True),
    StructField("name",StringType(), True),
    StructField("nationality",StringType(), True),
    StructField("url",StringType(), True)
])

# COMMAND ----------

df_constructors = spark.read \
    .format("json") \
    .option("Header", True) \
    .schema(constructors_schema) \
    .load(f"dbfs:{raw_folder_path}/constructors.json")
display(df_constructors)

# COMMAND ----------

df_constructors_raw = df_constructors \
                    .withColumnRenamed("constructorId","constructor_id") \
                    .withColumnRenamed("constructorRef","constructor_ref") \
                    .withColumn("ingestion_date", current_timestamp()) \
                    .withColumn("data_source",lit(v_data_source)) \
                    .drop("url")
df_constructors_raw = add_ingestion_date(df_constructors_raw)
display(df_constructors_raw)

# COMMAND ----------

df_constructors_raw.write.format("parquet").mode("overwrite").parquet("dbfs:/mnt/processed_databricksstoragef1/constructors")