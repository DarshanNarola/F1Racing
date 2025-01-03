# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 3. Call file system utlity mount to mount the storage
# MAGIC 4. Explore other file system utlities related to mount (list all mounts, unmount)

# COMMAND ----------

client_id = "8c3d21dc-af56-4068-9eae-20a7e24026df"
tenant_id = "f651bb08-d47e-4082-9eac-58ea3ae7ad1f"
client_secret = "11N8Q~6iqrH.MW8lFIk3eav8EUl54KqMNQhNfcYC"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@adlsracingf1.dfs.core.windows.net/",
  mount_point = "/mnt/adlsracingf1/demo",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@adlsracingf1.dfs.core.windows.net/",
  mount_point = "/mnt/adlsracingf1/raw",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://processed@adlsracingf1.dfs.core.windows.net/",
  mount_point = "/mnt/adlsracingf1/processed",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://presentation@adlsracingf1.dfs.core.windows.net/",
  mount_point = "/mnt/adlsracingf1/presentation",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/adlsracingf1/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/adlsracingf1/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/adlsracingf1/demo')

# COMMAND ----------

display(dbutils.fs.ls("/mnt/adlsracingf1/raw"))

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

