# Databricks notebook source
from pyspark.sql.functions import sum as _sum

def sum_dataframe_values(df):
    sum_columns = [ _sum(column).alias(column) for column in df.columns ]
    return df.select(sum_columns)

# COMMAND ----------


data = [("1", 10), ("2", 20), ("3", 30)]
columns = ["ID", "Value"]
df = spark.createDataFrame(data, columns)
display(df)
display(sum_dataframe_values(df))


# COMMAND ----------

import pyspark
print(pyspark.__version__)
