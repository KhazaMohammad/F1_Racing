# Databricks notebook source
# MAGIC %run "./config"

# COMMAND ----------

from pyspark.sql.functions import col, sum, when, count, rank

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentaion_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

constructor_standings_df = race_results_df \
    .groupBy(col("race_year"),col("team")) \
    .agg(sum(col("points")).alias("total_points"), count(when(col("position")==1, True)).alias("wins"))

constructor_standings_df.display()

# COMMAND ----------

from pyspark.sql.window import Window

window_spec = Window.partitionBy("race_year").orderBy(col("total_points").desc(),col("wins").desc())
final_df = constructor_standings_df.withColumn("rank", rank().over(window_spec))

# COMMAND ----------

final_df.write.format("parquet").mode("overwrite").save(f"{presentaion_folder_path}/constructor_standings")