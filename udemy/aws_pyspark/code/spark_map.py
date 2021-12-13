# Databricks notebook source
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('Read File')
sc = SparkContext.getOrCreate(conf=conf)
rdd = sc.textFile('sample.txt')

# COMMAND ----------

print(rdd.collect())

# COMMAND ----------

rdd2 = rdd.map(lambda x: x.split(' '))
print(rdd2.collect())

# COMMAND ----------

def foo(x):
    l = x.split() # -> ['1', '15', '8', '4']
    l2 = []
    for x in l:
        l2.append(int(x) ** 2)
    return l2

rdd3 = rdd.map(foo)
print(rdd3.collect())

# COMMAND ----------

#
