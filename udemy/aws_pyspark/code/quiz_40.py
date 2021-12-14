# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('quiz')
sc = SparkContext.getOrCreate(conf=conf)
rdd = sc.textFile('./FileStore/tables/quiz_40.txt')
print(rdd.collect())

# COMMAND ----------

print(rdd.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect())
