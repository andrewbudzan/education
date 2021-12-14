# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('average')
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/movie_ratings.csv')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.map(lambda x: (x.split(',')[0], (int(x.split(',')[1]), 1)))

# COMMAND ----------

rdd2.collect()

# COMMAND ----------

rdd3 = rdd2.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
rdd3.collect()

# COMMAND ----------

rdd4 = rdd3.map(lambda x: (x[0], round(x[1][0] / x[1][1], ndigits=2)))

# COMMAND ----------

rdd4.collect()

# COMMAND ----------

rdd2.
