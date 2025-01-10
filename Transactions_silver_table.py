# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

df_transactions=spark.sql('select * from demo.bronze_transactions')

# COMMAND ----------

# MAGIC %md
# MAGIC columns from df_transactions:
# MAGIC 1. id
# MAGIC 2. date
# MAGIC 3. client_id
# MAGIC 4. card_id
# MAGIC 5. merchant_id
# MAGIC 6. use_chip
# MAGIC 7. zip
# MAGIC 8. merchant_state
# MAGIC 9. amount

# COMMAND ----------

# MAGIC %md
# MAGIC ##Lets do some Transformation on the bronze table to convert transactions table into a silver table now

# COMMAND ----------

df_transactions_new = (
    df_transactions.select("*")
    .withColumn("amount", translate(col("amount"), "$", ""))
    .withColumnRenamed("amount", "Amount_in_$")
)
df_transactions_silver = (
    df_transactions_new.withColumn("id", col("id").cast("int"))
    .withColumnRenamed("id", "transaction_id")
    .withColumn("date", to_timestamp("date", "yyyy-MM-dd HH:mm:ss"))
    .withColumnRenamed("date", "transaction_date")
    .withColumn("client_id", col("client_id").cast("int"))
    .withColumn("merchant_id", col("merchant_id").cast("int"))
    .withColumn("zip", col("zip").cast("float"))
    .withColumn("Amount_in_$", col("Amount_in_$").cast("float"))
    .withColumn("mcc", col("mcc").cast("int").alias("MCC"))
    .select(
        "transaction_id",
        "transaction_date",
        "client_id",
        "merchant_id",
        "use_chip",
        "zip",
        "merchant_state",
        "merchant_city",
        "Amount_in_$",
        "MCC",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Checking null values, if present remove them.

# COMMAND ----------

df_transactions_silver.select("*").where(
    col("transaction_id").isNull()
    | col("transaction_date").isNull()
    | col("client_id").isNull()
    | col("merchant_id").isNull()
    | col("use_chip").isNull()
    | col("zip").isNull()
    | col("merchant_state").isNull()
    | col("merchant_city").isNull()
    | col("Amount_in_$").isNull()
    | col("MCC").isNull()
).count()
#There are total 1652706 rows which have null values in the transactions_silver table

# COMMAND ----------

df_transactions_silver_fin = df_transactions_silver.fillna(value="NA").fillna(value=0)

# COMMAND ----------

df_transactions_silver_fin.select("*").where(
    col("transaction_id").isNull()
    | col("transaction_date").isNull()
    | col("client_id").isNull()
    | col("merchant_id").isNull()
    | col("use_chip").isNull()
    | col("zip").isNull()
    | col("merchant_state").isNull()
    | col("merchant_city").isNull()
    | col("Amount_in_$").isNull()
    | col("MCC").isNull()
).count() #Removed all null values now

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing the data in delta table format now

# COMMAND ----------

df_transactions_silver_fin.write.format("delta").mode('overwrite').saveAsTable("demo.silver_transactions")
