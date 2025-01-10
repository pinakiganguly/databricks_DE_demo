# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

df_users=spark.sql('select * from demo.bronze_users')

# COMMAND ----------

# MAGIC %md
# MAGIC Fields from df_users;
# MAGIC 1. id
# MAGIC 2. current_age
# MAGIC 3. birth_month
# MAGIC 4. gender
# MAGIC 5. address
# MAGIC 6. credit score
# MAGIC 7. num_credit_cards

# COMMAND ----------

# MAGIC %md
# MAGIC ##Lets do some Transformation on the bronze table to convert user table into a silver table now

# COMMAND ----------

df_users_silver = (
    df_users.withColumn("id", col("id").cast("int"))
    .withColumnRenamed("id","user_id")
    .withColumn("current_age", col("current_age").cast("int"))
    .withColumn("birth_month", col("birth_month").cast("int"))
    .withColumn("address", col("address"))
    .withColumn("num_credit_cards", col("num_credit_cards").cast("int"))
    .withColumn("credit_score", col("credit_score").cast("int"))
    .drop("id")
    .select("user_id","current_age","birth_month","address","num_credit_cards","credit_score","gender",)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Checking null values in the fields
# MAGIC

# COMMAND ----------

df_users_silver.select("*").where(
    col("user_id").isNull()
    | col("current_age").isNull()
    | col("birth_month").isNull()
    | col("address").isNull()
    | col("num_credit_cards").isNull()
    | col("credit_score").isNull()
).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Writing into delta table

# COMMAND ----------

df_users_silver.write.format("delta").mode('overwrite').saveAsTable("demo.silver_users")
