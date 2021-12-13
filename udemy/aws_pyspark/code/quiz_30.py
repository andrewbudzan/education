# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('Quiz_30')
sc = SparkContext.getOrCreate(conf=conf)
rdd = sc.textFile('./FileStore/tables/quiz_30.txt')

# COMMAND ----------

def count_letters(x):
    words = x.split(' ')
    return [len(word) for word in words]

# COMMAND ----------

rdd2 = rdd.map(count_letters)
print(rdd2.collect())

# COMMAND ----------

rdd3 = rdd.map(lambda x: x.split(' ')).map(lambda x: [len(w) for w in x])
print(rdd3.collect())

# COMMAND ----------

rdd4 = rdd.map(lambda x: [len(w) for w in x.split(' ')])
print(rdd4.collect())
