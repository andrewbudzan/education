# Databricks notebook source
from pyspark import SparkConf, SparkContext

# COMMAND ----------

conf = SparkConf().setAppName('Read File')

# COMMAND ----------

sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

text = sc.textFile('./FileStore/tables/sample.txt')

# text - its a transformation, which currently doesnt make any transformations or operations
# to perfrom this operation we need to `call` an action

# COMMAND ----------

# text is only a reference to worker, but not the result of transformations

# COMMAND ----------

print(text.collect())
