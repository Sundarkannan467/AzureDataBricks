-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ### deletion vectors with git integration

-- COMMAND ----------

show databases

-- COMMAND ----------

show tables in default

-- COMMAND ----------

use default;
select * from customers_data1 limit 10

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/user/hive/warehouse/customers_data1"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC SASkeyvalue=dbutils.secrets.get(scope="adb_keyvault_scope1",key="adbuatstorageaccountSAStocken")
-- MAGIC spark.conf.set("fs.azure.account.auth.type.adbuatstorageaccount.dfs.core.windows.net", "SAS")
-- MAGIC spark.conf.set("fs.azure.sas.token.provider.type.adbuatstorageaccount.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")            
-- MAGIC spark.conf.set("fs.azure.sas.fixed.token.adbuatstorageaccount.dfs.core.windows.net",SASkeyvalue)
-- MAGIC
-- MAGIC display(dbutils.fs.ls("abfss://bronze@adbuatstorageaccount.dfs.core.windows.net"))
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df=spark.read.csv(path="abfss://bronze@adbuatstorageaccount.dfs.core.windows.net/business-operations-survey-2023-business-operations.csv",sep=",",header=True,inferSchema=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.count()
-- MAGIC display(df)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").partitionBy("industry").format("delta").saveAsTable("operations")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/user/hive/warehouse/operations"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").format("delta").saveAsTable("operations1")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/user/hive/warehouse/operations1"))

-- COMMAND ----------

select * from operations1;
delete from operations1
where industry='Publishing'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/user/hive/warehouse/operations1"))

-- COMMAND ----------

select * from operations1;
delete from operations1
where industry='Agriculture'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/user/hive/warehouse/operations1"))

-- COMMAND ----------

select * from operations1;
delete from operations1
where industry='Insurance'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/user/hive/warehouse/operations1"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### optimize command will merge all the deletion vector files together and rewrite the new parquet file into hive and old files will be intact

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("OPTIMIZE operations1")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/user/hive/warehouse/operations1"))

-- COMMAND ----------

ALTER TABLE operations1 
SET tblproperties ('comment' = 'Operations table for Insurance');

-- COMMAND ----------

ALTER TABLE operations1 
SET tblproperties ('delta.enableDeletionVectors' = false);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/user/hive/warehouse/operations1"))

-- COMMAND ----------

select * from operations1;
delete from operations1
where industry='Construction'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/user/hive/warehouse/operations1"))
