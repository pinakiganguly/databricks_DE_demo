# Databricks notebook source
from pyspark.sql.functions import *

df_users_silver=spark.sql('select * from demo.silver_users')
df_transact_silver=spark.sql('select * from demo.silver_transactions')
df_cards_silver=spark.sql('select * from demo.silver_cards')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Joining the cards and users table first

# COMMAND ----------

df_users_silver.count()

# COMMAND ----------

df_cards_silver.count()

# COMMAND ----------

df_transact_pre_gold = (
    df_cards_silver.join(
        df_users_silver, df_cards_silver.client_id == df_users_silver.user_id, "left"
    )
    .drop("client_id")
    .withColumn(
        "Encrypted_card_number",
        expr("substr(card_number, 1,6) || 'XXXXXXXXX' || substr(card_number, -4)"),
    )
    .drop("card_number")
    .withColumnRenamed("Encrypted_card_number", "card_number")
    .select(
        "user_id",
        "current_age",
        "birth_month",
        "address",
        "num_credit_cards",
        "credit_score",
        "gender",
        "card_id",
        "card_brand",
        "card_type",
        "has_chip",
        "card_number",
        "expires",
        "Credit_limit_in_$",
        "card_on_dark_web",
    )
)

# COMMAND ----------

df_transact_pre_gold.display()

# COMMAND ----------

df_transact_gold=df_transact_silver.join(df_transact_pre_gold, df_transact_silver.client_id==df_transact_pre_gold.user_id, "left").select(
        "user_id",
        "card_id",
        "transaction_id",
        "merchant_id",
        "current_age",
        "birth_month",
        "address",
        "num_credit_cards",
        "credit_score",
        "gender",
        "card_brand",
        "card_type",
        "has_chip",
        "card_number",
        "expires",
        "Credit_limit_in_$",
        "card_on_dark_web",
        "transaction_date",
        "use_chip",
        "zip",
        "merchant_state",
        "merchant_city",
        "Amount_in_$",
        "MCC"
    ).drop('client_id').dropDuplicates()

# COMMAND ----------

df_transact_silver.count()

# COMMAND ----------

df_transact_gold.count()

# COMMAND ----------

df_transact_gold.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Finally writing this to a delta table

# COMMAND ----------

df_transact_gold.write.format("delta").mode("overwrite").saveAsTable(
    "demo.card_transactions_gold"
)
