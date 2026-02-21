# Databricks notebook source
# MAGIC %md
# MAGIC ####Requirement
# MAGIC 1. Read raw data from flight_time_raw table
# MAGIC 2. Apply transformations to time values as hour to minute interval
# MAGIC
# MAGIC     1. CRS_DEP_TIME
# MAGIC     2. DEP_TIME
# MAGIC     3. WHEELS_ON
# MAGIC     4. CRS_ARR_TIME
# MAGIC     5. ARR_TIME
# MAGIC 3. Apply transformation to TAXI_IN to make it a minute interval

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dev.spark_db.flight_time_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Read data to create a dataframe

# COMMAND ----------

flight_time_raw_df = spark.read.table("dev.spark_db.flight_time_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Develop logic to transform CRS_DEP_TIME to an interval

# COMMAND ----------

from pyspark.sql.functions import expr
from pyspark.sql import functions as F
step_1_df = (
    flight_time_raw_df.withColumns({
    "CRS_DEP_TIME_HH": expr("left(lpad(CRS_DEP_TIME, 4, '0'), 2)"),
    "CRS_DEP_TIME_MM": expr("right(lpad(CRS_DEP_TIME, 4, '0'), 2)"),
    })
)

step_2_df = (
    step_1_df.withColumns({
        "CRS_DEP_TIME_NEW": expr("cast(concat(CRS_DEP_TIME_HH, ':', CRS_DEP_TIME_MM) AS INTERVAL HOUR TO MINUTE)")
    })
)

step3_upper_df = step_2_df.withColumn("ORIGIN_CITY_NAME",expr("upper(ORIGIN_CITY_NAME)"))

step4_lower_df = step_2_df.withColumn("DEST_CITY_NAME",expr("lower(DEST_CITY_NAME)"))

step_case_df = step4_lower_df.withColumn("DISTANCE_EX",expr("CASE WHEN DISTANCE >600 THEN 'LONG DISTANCE' ELSE 'SHORT DISTANCE' END"))  


step_5_col = step_case_df.select("*",F.col("DEST_CITY_NAME"),F.lit("USA").alias("country"))


# COMMAND ----------

step_5_col.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Develop a reusable function

# COMMAND ----------

 def get_interval(hhmm_value):
     from pyspark.sql.functions import expr

     return expr(f"""
                 cast(concat(left(lpad({hhmm_value}, 4, '0'), 2), ':', 
                             right(lpad({hhmm_value}, 4, '0'), 2)) 
                             AS INTERVAL HOUR TO MINUTE)
                 """)

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Apply function to dataframe

# COMMAND ----------

result_df = (
    flight_time_raw_df.withColumns({
        "CRS_DEP_TIME": get_interval("CRS_DEP_TIME"),
        "DEP_TIME": get_interval("DEP_TIME"),
        "WHEELS_ON": get_interval("WHEELS_ON"),
        "CRS_ARR_TIME": get_interval("CRS_ARR_TIME"),
        "ARR_TIME": get_interval("ARR_TIME"),
        "TAXI_IN": expr("cast(TAXI_IN AS INTERVAL MINUTE)")
    })
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Save results to the table 

# COMMAND ----------

result_df.write.mode("overwrite").saveAsTable("dev.spark_db.flight_time")

# COMMAND ----------


