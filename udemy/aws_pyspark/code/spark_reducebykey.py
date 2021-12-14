# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('groupby')
sc = SparkContext.getOrCreate(conf=conf)
rdd = sc.textFile('./FileStore/tables/sample.txt')

# COMMAND ----------

print(rdd.collect())

# COMMAND ----------

print(rdd.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect())

# COMMAND ----------
