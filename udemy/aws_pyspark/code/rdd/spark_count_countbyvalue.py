# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('quiz')
sc = SparkContext.getOrCreate(conf=conf)
rdd = sc.textFile('./FileStore/tables/quiz_40.txt')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split(' '))

# COMMAND ----------

rdd2.count()

# COMMAND ----------

rdd2.countByValue()
