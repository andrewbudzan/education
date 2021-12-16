# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('min max quiz')
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/average_quiz_sample.csv')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.map(lambda x: (x.split(',')[1], float(x.split(',')[2])))
rdd2.collect()

# COMMAND ----------

# min rating given by city
rdd_min = rdd2.reduceByKey(lambda x, y: x if x < y else y)
rdd_min.collect()

# COMMAND ----------

# max rating given by city
rdd_max = rdd2.reduceByKey(lambda x, y: x if x > y else y)
rdd_max.collect()
