# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('average quiz')
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/average_quiz_sample.csv')
rdd.collect()

# COMMAND ----------

# rdd2 = rdd.map(lambda x: (x.split(',')[0], (x.split(',')[1], float(x.split(',')[2]))))
rdd2 = rdd.map(lambda x: (x.split(',')[0], (float(x.split(',')[2]), 1))) 
rdd2.collect()

# COMMAND ----------

rdd2.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda x: (x[0], x[1][0] / x[1][1])).collect()
