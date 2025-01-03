-- Databricks notebook source
drop database f1_processed cascade

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/adlsracingf1/processed"

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------

drop database f1_presentation cascade

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/adlsracingf1/presentation"