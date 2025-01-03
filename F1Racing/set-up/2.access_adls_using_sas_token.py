# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.adlsracingf1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.adlsracingf1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.adlsracingf1.dfs.core.windows.net", "sp=rl&st=2024-12-29T18:09:49Z&se=2025-01-11T02:09:49Z&spr=https&sv=2022-11-02&sr=c&sig=7PiFy%2FElUIJgoUYjDDTdihfu5QD06uAHKqH8zoTMNqA%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@adlsracingf1.dfs.core.windows.net"))

# COMMAND ----------

spark.read.format("csv").option("header",True).load("abfss://demo@adlsracingf1.dfs.core.windows.net/circuits.csv").display()

# COMMAND ----------

