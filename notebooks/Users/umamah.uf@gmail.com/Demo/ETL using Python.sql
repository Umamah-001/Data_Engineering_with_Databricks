-- Databricks notebook source
-- MAGIC %md
-- MAGIC <img src="https://adatis.co.uk/wp-content/uploads/og-databricks.png" alt="Girl in a jacket" style="width:100px;height:50px;">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Markdown
-- MAGIC 
-- MAGIC The magic command **&percnt;md** allows us to render Markdown in a cell:
-- MAGIC Double click this cell to begin editing it. Then hit **`Esc`** to stop editing
-- MAGIC # Title One
-- MAGIC ## Title Two
-- MAGIC ### Title Three

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Bold Text: **bold** 
-- MAGIC 
-- MAGIC Italicized Text: *italicized*
-- MAGIC 
-- MAGIC Ordered List:
-- MAGIC 1. once
-- MAGIC 1. two
-- MAGIC 1. three
-- MAGIC 
-- MAGIC Unordered List:
-- MAGIC * apples
-- MAGIC * peaches
-- MAGIC * bananas
-- MAGIC 
-- MAGIC Links/Embedded HTML: <a href="https://community.cloud.databricks.com/login.html" target="_blank">Databricks Community Edition</a>
-- MAGIC 
-- MAGIC Tables:
-- MAGIC 
-- MAGIC | name   | value |
-- MAGIC |--------|-------|
-- MAGIC | A      | 1     |
-- MAGIC | B      | 2     |
-- MAGIC | C      | 3     |

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import sys
-- MAGIC from pyspark.sql import *  # SparkSession, Row
-- MAGIC from pyspark.sql.functions import *  # month, year, col, lit, to_date, when,to_timestamp, date_format
-- MAGIC from pyspark.sql.types import * #IntegerType,FloatType,DoubleType
-- MAGIC from pyspark.sql.window import *  # Window
-- MAGIC from datetime import date
-- MAGIC from pyspark.sql.types import DoubleType
-- MAGIC from dateutil.relativedelta import relativedelta
-- MAGIC import pyspark.sql.functions as sf
-- MAGIC import pyspark.sql.types as sparktypes
-- MAGIC import datetime
-- MAGIC import pandas as pd
-- MAGIC spark = SparkSession.builder.appName("Demo").getOrCreate()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Mount Storage Account
-- MAGIC * You can mount azure blob storage in databricks by specifying the storage account name, container name and access token.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mount(
-- MAGIC   source = "wasbs://demo@umamah.blob.core.windows.net",
-- MAGIC   mount_point = "/mnt/demo",
-- MAGIC   extra_configs = {"fs.azure.account.key.umamah.blob.core.windows.net":"tfgyBV5ScODQT7Nh/xVACX3Jyr02mVepBEZ92dzfpVQnjkIuixplxF1tz4/+lFJ9MKQr/FaergQa+AStAfaJCg=="})

-- COMMAND ----------

-- MAGIC %python
-- MAGIC /ls/dbfs/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # ETL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Read Data from Blob

-- COMMAND ----------

-- MAGIC %python
-- MAGIC blob_input = spark.read.option("header",True).csv("/mnt/demo/Sales_Data.csv")
-- MAGIC display(blob_input)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Read Data from Databricks File System

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbfs_input = spark.read.format("csv").option("header","true").load("/FileStore/Sales_Data.csv")
-- MAGIC display(dbfs_input) ## spark dataframe

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Transformation

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = dbfs_input.toPandas() ## convert spark dataframe to pandas
-- MAGIC df['Date'] = pd.to_datetime(df["Date"],infer_datetime_format=True)
-- MAGIC df['Date'] = df['Date'] - pd.to_timedelta(df['Date'].dt.day - 1, unit='d')
-- MAGIC df["Price"] = df["Price"].astype(int)
-- MAGIC df["Discount"] = df["Discount"].astype(float)
-- MAGIC df["Quantity Sold"] = df["Quantity Sold"].astype(int)
-- MAGIC df["Sales"] = (df["Price"] - (df["Price"] * df["Discount"])) * df["Quantity Sold"]
-- MAGIC df = df.drop(columns=["Price","Discount","Quantity Sold"])
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Load Results

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_grouped = df.groupby(["Region","Country","Category","Brand","Date"]).sum("Sales").reset_index()
-- MAGIC display(df_grouped)
-- MAGIC df_grouped_spark = spark.createDataFrame(df_grouped)
-- MAGIC df_grouped_spark.write.mode("append").saveAsTable("Sales")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query Delta Tables

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from Sales

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # %run
-- MAGIC * You can run a notebook from another notebook by using the magic command **%run**
-- MAGIC * Notebooks to be run are specified with relative paths
-- MAGIC * The referenced notebook executes as if it were part of the current notebook, so temporary views and other local declarations will be available from the calling notebook

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(name)

-- COMMAND ----------

-- MAGIC %run /Users/umamah.uf@gmail.com/Demo/Test

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Check all mount points

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mounts()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Unmount Storage Account

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.unmount("/mnt/demo") 

-- COMMAND ----------

