# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

df_cards=spark.sql('select * from demo.bronze_cards')

# COMMAND ----------

# MAGIC %md
# MAGIC Columns needed from df_cards:
# MAGIC 1. id
# MAGIC 2. client_id
# MAGIC 3. card_brand
# MAGIC 4. card_type
# MAGIC 5. card_number
# MAGIC 6. expires
# MAGIC 7. cvv
# MAGIC 8. credit_limit
# MAGIC 9. card_on_dark_web 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Lets do some Transformation on the bronze table to convert cards table into a silver table now

# COMMAND ----------

df_cards_new = (
    df_cards.select("*")
    .withColumn("credit_limit", translate(col("credit_limit"), "$", ""))
    .withColumnRenamed("credit_limit", "Credit_limit_in_$")
)
df_cards_silver = (
    df_cards_new.withColumn("id", col("id").cast("int"))
    .withColumnRenamed('id','card_id')
    .withColumn("client_id", col("client_id").cast("int"))
    .withColumn("cvv", col("cvv").cast("int"))
    .withColumn("Credit_limit_in_$", col("Credit_limit_in_$").cast("int"))
    .select(
        "card_id",
        "client_id",
        "card_brand",
        "card_type",
        "has_chip",
        "card_number",
        "expires",
        "cvv",
        "Credit_limit_in_$",
        "card_on_dark_web",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Checking null values, if present remove them.

# COMMAND ----------

df_cards_silver.select("*").where(
    col("card_id").isNull()
    | col("client_id").isNull()
    | col("card_brand").isNull()
    | col("card_type").isNull()
    | col("has_chip").isNull()
    | col("card_number").isNull()
    | col("expires").isNull()
    | col("cvv").isNull()
    | col("Credit_limit_in_$").isNull()
    | col("card_on_dark_web").isNull()
).count() #Has no null values

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing the data in delta table format now

# COMMAND ----------

df_cards_silver.write.format("delta").mode('overwrite').saveAsTable("demo.silver_cards")

# COMMAND ----------


