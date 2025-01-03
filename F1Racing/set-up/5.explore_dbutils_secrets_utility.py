# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'adlsracingf1-scope')

# COMMAND ----------

dbutils.secrets.get(scope ='adlsracingf1-scope', key ='adlsracing-accesskey')