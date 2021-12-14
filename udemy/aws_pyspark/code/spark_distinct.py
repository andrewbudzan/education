# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('flatmap')
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('./FileStore/tables/sample.txt')
print(rdd.collect())

# COMMAND ----------

rdd2 = rdd.distinct()
print(rdd2.collect())

# COMMAND ----------

rdd3 = rdd.flatMap(lambda x: x.split(' '))
print(rdd3.collect())

# COMMAND ----------

rdd4 = rdd3.distinct()
print(rdd4.collect())

# COMMAND ----------

print(rdd.flatMap(lambda x: x.split(' ')).distinct().collect())
