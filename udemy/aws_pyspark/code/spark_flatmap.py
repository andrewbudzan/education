# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('flatmap')
sc = SparkContext.getOrCreate(conf=conf)
rdd = sc.textFile('/FileStore/tables/sample.txt')

# COMMAND ----------

print(rdd.collect())

# COMMAND ----------

rdd2 = rdd.map(lambda x: x.split(' '))
print(rdd2.collect())

# Output:
# [['1', '15', '8', '4'],
#  ['8', '6', '44', '2'],
#  ['1', '8', '3', '48'],
#  ['57', '1', '44', '5']]

# COMMAND ----------

rdd3 = rdd.flatMap(lambda x: x.split(' '))
print(rdd3.collect())

# Output:
# ['1', '15', '8', '4', '8', '6', '44', '2', '1', '8', '3', '48', '57', '1', '44', '5']
