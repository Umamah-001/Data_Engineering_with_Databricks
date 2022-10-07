# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://adatis.co.uk/wp-content/uploads/og-databricks.png" alt="Girl in a jacket" style="width:100px;height:50px;">

# COMMAND ----------

# MAGIC %md
# MAGIC # Markdown
# MAGIC 
# MAGIC The magic command **&percnt;md** allows us to render Markdown in a cell:
# MAGIC Double click this cell to begin editing it. Then hit **`Esc`** to stop editing
# MAGIC # Title One
# MAGIC ## Title Two
# MAGIC ### Title Three

# COMMAND ----------

# MAGIC %md
# MAGIC Bold Text: **bold** 
# MAGIC 
# MAGIC Italicized Text: *italicized*
# MAGIC 
# MAGIC Ordered List:
# MAGIC 1. once
# MAGIC 1. two
# MAGIC 1. three
# MAGIC 
# MAGIC Unordered List:
# MAGIC * apples
# MAGIC * peaches
# MAGIC * bananas
# MAGIC 
# MAGIC Links/Embedded HTML: <a href="https://community.cloud.databricks.com/login.html" target="_blank">Databricks Community Edition</a>
# MAGIC 
# MAGIC Tables:
# MAGIC 
# MAGIC | name   | value |
# MAGIC |--------|-------|
# MAGIC | A      | 1     |
# MAGIC | B      | 2     |
# MAGIC | C      | 3     |

# COMMAND ----------

import sys
from pyspark.sql import *  # SparkSession, Row
from pyspark.sql.functions import *  # month, year, col, lit, to_date, when,to_timestamp, date_format
from pyspark.sql.types import * #IntegerType,FloatType,DoubleType
from pyspark.sql.window import *  # Window
from datetime import date
from pyspark.sql.types import DoubleType
from dateutil.relativedelta import relativedelta
import pyspark.sql.functions as sf
import pyspark.sql.types as sparktypes
import datetime
import pandas as pd
spark = SparkSession.builder.appName("Demo").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC # Mount Storage Account
# MAGIC * You can mount azure blob storage in databricks by specifying the storage account name, container name and access token.

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://demo@umamah.blob.core.windows.net",
  mount_point = "/mnt/demo",
  extra_configs = {"fs.azure.account.key.umamah.blob.core.windows.net":"tfgyBV5ScODQT7Nh/xVACX3Jyr02mVepBEZ92dzfpVQnjkIuixplxF1tz4/+lFJ9MKQr/FaergQa+AStAfaJCg=="})

# COMMAND ----------

/ls/dbfs/

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from Blob

# COMMAND ----------

blob_input = spark.read.option("header",True).csv("/mnt/demo/Sales_Data.csv")
display(blob_input)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from Databricks File System

# COMMAND ----------

dbfs_input = spark.read.format("csv").option("header","true").load("/FileStore/Sales_Data.csv")
display(dbfs_input) ## spark dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation

# COMMAND ----------

df = dbfs_input.toPandas() ## convert spark dataframe to pandas
df['Date'] = pd.to_datetime(df["Date"],infer_datetime_format=True)
df['Date'] = df['Date'] - pd.to_timedelta(df['Date'].dt.day - 1, unit='d')
df["Price"] = df["Price"].astype(int)
df["Discount"] = df["Discount"].astype(float)
df["Quantity Sold"] = df["Quantity Sold"].astype(int)
df["Sales"] = (df["Price"] - (df["Price"] * df["Discount"])) * df["Quantity Sold"]
df = df.drop(columns=["Price","Discount","Quantity Sold"])
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Results

# COMMAND ----------

df_grouped = df.groupby(["Region","Country","Category","Brand","Date"]).sum("Sales").reset_index()
display(df_grouped)
df_grouped_spark = spark.createDataFrame(df_grouped)
df_grouped_spark.write.mode("overwrite").saveAsTable("Sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Delta Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Sales

# COMMAND ----------

# MAGIC %md 
# MAGIC # %run
# MAGIC * You can run a notebook from another notebook by using the magic command **%run**
# MAGIC * Notebooks to be run are specified with relative paths
# MAGIC * The referenced notebook executes as if it were part of the current notebook, so temporary views and other local declarations will be available from the calling notebook

# COMMAND ----------

print(name)

# COMMAND ----------

# MAGIC %run /Users/umamah.uf@gmail.com/Demo/Test

# COMMAND ----------

# MAGIC %md
# MAGIC # Check all mount points

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC # Unmount Storage Account

# COMMAND ----------

dbutils.fs.unmount("/mnt/demo") 

# COMMAND ----------

