# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('groupby')
sc = SparkContext.getOrCreate(conf=conf)
rdd = sc.textFile('./FileStore/tables/sample_words.txt')

# COMMAND ----------

print(rdd.collect())

# COMMAND ----------

rdd.map(lambda x: (x, len(x.split(' ')))).collect()

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split(' ')).map(lambda x: (x, len(x)))

# COMMAND ----------

print(rdd2.groupByKey().mapValues(list).collect())

# COMMAND ----------

rdd3 = rdd.flatMap(lambda x: x.split(' ')).map(lambda x: (len(x), x))

# COMMAND ----------

print(rdd3.groupByKey().mapValues(list).collect())

# COMMAND ----------

print(rdd3.groupByKey().mapValues(list).collect())
