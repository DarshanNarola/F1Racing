# Databricks notebook source
# MAGIC %md
# MAGIC Access Azure Data Lake using access keys
# MAGIC - Set the spark config fs.azure.account.key
# MAGIC - List files from demo container
# MAGIC - Read data from circuits.csv file

# COMMAND ----------

key_scope_store = dbutils.secrets.get(scope ='adlsracingf1-scope', key ='adlsracing-accesskey')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.adlsracingf1.dfs.core.windows.net",
    key_scope_store
)

# COMMAND ----------

dbutils.fs.ls('abfss://demo@adlsracingf1.dfs.core.windows.net/')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.adlsracingf1.dfs.core.windows.net",
    "momAM4f8TWW2rIuemXemgydgoUwkT4llD6uh1461LAFwTUFv0TO/bZ0Rzi0+MxFi8YkGt20hEAU9+ASt7zkYaw=="
)

# COMMAND ----------

spark.read.format("csv").option("header",True).load('abfss://demo@adlsracingf1.dfs.core.windows.net/circuits.csv').display()