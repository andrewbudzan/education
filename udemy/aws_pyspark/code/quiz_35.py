# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('quiz_35')
sc = SparkContext.getOrCreate(conf=conf)
rdd = sc.textFile('quiz_35.txt')

# COMMAND ----------

print(rdd.collect())

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split(' ')).filter(lambda x: not (x.startswith('a') or x.startswith('c')))
print(rdd2.collect())

# Out[8]: ['this', 'mango', 'dog', 'mic', 'laptop', 'switch', 'mobile']

# COMMAND ----------


def func_map(x):
    return x.split(' ')


def filter_words(x):
    return True if not (x.startswith('a') or x.startswith('c')) else False


rdd3 = rdd.flatMap(func_map).filter(filter_words)
print(rdd3.collect())

# Out[14]: ['this', 'mango', 'dog', 'mic', 'laptop', 'switch', 'mobile']
