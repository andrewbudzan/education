# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('quiz')
sc = SparkContext.getOrCreate(conf=conf)
rdd = sc.textFile('./FileStore/tables/quiz_40.txt')

# COMMAND ----------

rdd.saveAsTextFile('./FileStore/tables/sample_words2.txt')

# COMMAND ----------

# rdd.getNumPartitions()

# COMMAND ----------

# rdd = sc.textFile('./FileStore/tables/words')

# COMMAND ----------

# rdd3 = rdd.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1))

# COMMAND ----------

# rdd3.saveAsTextFile(f'C:/Users/andbud/projects/education/udemy/aws_pyspark/code/FileStore/tables/words')
