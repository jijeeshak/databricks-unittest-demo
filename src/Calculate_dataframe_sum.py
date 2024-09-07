# Databricks notebook source
from pyspark.sql.functions import sum as _sum

def sum_dataframe_values(df):
    sum_columns = [ _sum(column).alias(column) for column in df.columns ]
    return df.select(sum_columns)

# COMMAND ----------

df2=sum_dataframe_values(spark.range(10))
display(df2)

# COMMAND ----------

df1=spark.range(10)
display(df1)
