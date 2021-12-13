# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('flatmap')
sc = SparkContext.getOrCreate(conf=conf)
rdd = sc.textFile('sample.txt')

# COMMAND ----------

print(rdd.collect())

# COMMAND ----------

rdd2 = rdd.filter(lambda x: x != '1 8 3 48')
print(rdd2.collect())

# Out[4]: ['1 15 8 4', '8 6 44 2', '57 1 44 5']

# COMMAND ----------


def fltr(x):
    if sum([int(el) for el in x.split(' ')]) <= 28:
        return False
    else:
        return True


def fltr2(x):
    return False if sum([int(el) for el in x.split(' ')]) <= 28 else True


rdd3 = rdd.filter(fltr)
print(rdd3.collect())


# Out[30]: ['8 6 44 2', '1 8 3 48', '57 1 44 5']

# COMMAND ----------
