# Databricks notebook source
# MAGIC %run "../config"

# COMMAND ----------

from pyspark.sql.functions import col, current_date, current_timestamp, concat, concat_ws, to_timestamp, lit, explode
from pyspark.sql.types import *

# COMMAND ----------

drivers_schema = StructType([
    StructField("code",StringType(),False),
    StructField("dob",StringType(),True),
    StructField("driverId",IntegerType(), True),
    StructField("driverRef",StringType(), True),
    StructField("name",StructType([StructField("forename",StringType(),False),StructField("surname",StringType(),False)])),
    StructField("nationality",StringType(), True),
    StructField("number",StringType(), True),
    StructField("url",StringType(), True)
])

# COMMAND ----------

df_drivers = spark.read \
    .format("json") \
    .option("Header", True) \
    .schema(drivers_schema) \
    .load(f"dbfs:{raw_folder_path}/drivers.json")
display(df_drivers)

# COMMAND ----------

df_drivers_raw = df_drivers \
                    .withColumnRenamed("driverId","driver_id") \
                    .withColumnRenamed("driverRef","driver_ref") \
                    .withColumn("name", concat(col("name.forename") , lit(" "), col("name.surname"))) \
                    .withColumn("ingestion_date", current_timestamp()) \
                    .withColumn("data_source",lit(v_data_source)) \
                    .drop("url")
df_drivers_raw = add_ingestion_date(df_drivers_raw)
display(df_drivers_raw)

# COMMAND ----------

df_drivers_raw.write.format("parquet").mode("overwrite").parquet("dbfs:/mnt/processed_databricksstoragef1/drivers")