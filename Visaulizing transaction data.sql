-- Databricks notebook source
-- DBTITLE 1,Users from particular address whose age is 40yrs
select
  distinct user_id,
  card_id,
  address,
  credit_score,
  current_age
from
  hive_metastore.demo.card_transactions_gold
where
  current_age = 40;

-- COMMAND ----------

-- DBTITLE 1,Maximum credit score of each users
select
  user_id,
  max(credit_score) as Max_credit_score
from
  hive_metastore.demo.card_transactions_gold
group by
  user_id
order by
  user_id;

-- COMMAND ----------

-- DBTITLE 1,No of transactions done from merchants of particular city
select
  count(transaction_id) as Number_of_transactions,
  merchant_city
from
  hive_metastore.demo.card_transactions_gold
group by
  merchant_city
order by
  Number_of_transactions desc;

-- COMMAND ----------

-- DBTITLE 1,Maximum transaction done by user  and of which card company
select
  user_id,
  card_brand,
  max(`Amount_in_$`) as Maximum_transaction_amount
from
  hive_metastore.demo.card_transactions_gold
group by
  user_id,
  card_brand
order by
  Maximum_transaction_amount desc;

-- COMMAND ----------

-- Find users who have made transactions in the city starting with letter N and have a card number starting with 5 and are from the zip codes is not equal to 0
select
  user_id,
  transaction_date
from
  hive_metastore.demo.card_transactions_gold
where
  card_number like('5%')
  and zip in (
    select
      zip
    from
      hive_metastore.demo.card_transactions_gold
    where
      zip <> 0
  )
  and merchant_city in(
    select
      merchant_city
    from
      hive_metastore.demo.card_transactions_gold
    where
      merchant_city like('N%')
  );

-- COMMAND ----------


