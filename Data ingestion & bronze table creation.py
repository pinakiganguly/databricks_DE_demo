# Databricks notebook source
df_users=spark.read.csv('dbfs:/FileStore/Pinaki/users_data.csv',header=True)
df_transactions=spark.read.csv('dbfs:/FileStore/Pinaki/transactions_data.csv',header=True)
df_cards=spark.read.csv('dbfs:/FileStore/Pinaki/cards_data.csv',header=True)

# COMMAND ----------

df_users.write.format("delta").mode('overwrite').saveAsTable("demo.bronze_users")
df_transactions.write.format("delta").mode('overwrite').saveAsTable("demo.bronze_transactions")
df_cards.write.format("delta").mode('overwrite').saveAsTable("demo.bronze_cards")
