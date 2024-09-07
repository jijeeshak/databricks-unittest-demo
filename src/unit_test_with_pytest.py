# Databricks notebook source
# MAGIC %pip install pytest=="8.3.2"

# COMMAND ----------

# MAGIC %run ./Calculate_dataframe_sum

# COMMAND ----------

class TestSumDataframeValues:
    #create a dataframe
    @staticmethod
    def create_dataframe_with_2_cols():
        data = [("101", 1), ("201", 2), ("301", 3)]
        columns = ["ID", "Value"]
        df = spark.createDataFrame(data, schema=columns)
        return df

    # Pytest unit test - scenario-1
    def test1_sum_dataframe_values(self):
        df = self.create_dataframe_with_2_cols()
        result = sum_dataframe_values(df.select("ID")).collect()[0][0]
        assert result == 603, "The sum of the IDs should be 603"

    # Pytest unit test - scenario-2
    def test2_sum_dataframe_values(self):
        df = self.create_dataframe_with_2_cols()
        result = sum_dataframe_values(df.select("Value")).collect()[0][0]
        assert result == 6, "The sum of the values should be 6"

    def test3_sum_dataframe_values(self):
        from pyspark.testing import assertDataFrameEqual
        from pyspark.sql.types import StructType, StructField, LongType, DoubleType
        df= self.create_dataframe_with_2_cols()
        result = sum_dataframe_values(df.select("ID", "Value"))
        schemaVal = StructType([StructField("ID", DoubleType(), True), StructField("Value", LongType(), True)])    
        expected_result = spark.createDataFrame([(603.0, 6)], schema=schemaVal)
        assertDataFrameEqual(result, expected_result, "The sum of the IDs should be 603 and values should be 6")

# COMMAND ----------

test_instance = TestSumDataframeValues()
test_instance.test1_sum_dataframe_values()
test_instance.test2_sum_dataframe_values()
test_instance.test3_sum_dataframe_values()
