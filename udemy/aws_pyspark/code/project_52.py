# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('Students Project')
sc = SparkContext.getOrCreate(conf=conf).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Students Project
# MAGIC ### tasks:
# MAGIC   - number of students
# MAGIC   - total marks achieved by Female and Male students
# MAGIC   - total number of students that have passed and failed. 50+ required to pass
# MAGIC   - total number of students enrolled per cource
# MAGIC   - total marks that students achieved per cource
# MAGIC   - average marks that students achieved per cource
# MAGIC   - min and max marks that students achieved per cource
# MAGIC   - average age of Male and Female students

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/StudentData.csv')
headers = rdd.first()
rdd = rdd.filter(lambda x: x != headers)

# COMMAND ----------

headers

# COMMAND ----------

rdd.collect()
