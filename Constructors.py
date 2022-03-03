# Databricks notebook source
# MAGIC %md 
# MAGIC sparkSession initilozation

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Constructors").getOrCreate()

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING,nationality STRING ,url STRING "

# COMMAND ----------

constructor_DF = spark.read.schema(constructor_schema).json("/FileStore/tables/constructors-1.json")


# COMMAND ----------

# MAGIC %md
# MAGIC Drop url column

# COMMAND ----------

constructor_dropcolDF = constructor_DF.drop(constructor_DF["url"])


# COMMAND ----------

# MAGIC %md
# MAGIC rename columns and add ingstion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_finalDF = constructor_dropcolDF.withColumnRenamed("constructorId", "constructor_id")\
                                           .withColumnRenamed("construcorRef","constructor_ref")\
                                           .withColumn("ingestion_date" , current_timestamp())



# COMMAND ----------

constructor_finalDF.write.mode("overwrite").parquet("/FileStore/tables/output.45/constructorsoutput")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/output.45/constructorsoutput
