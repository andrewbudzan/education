# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('repartition')
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd.getNumPartitions()

# COMMAND ----------

rdd = sc.textFile('./FileStore/tables/words')
rdd = rdd.repartition(5)
rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.map(lambda x: (x, 1))

# COMMAND ----------

rdd3.saveAsTextFile('./FileStore/tables/output/5partition/')

# COMMAND ----------

rdd = sc.textFile('./FileStore/tables/words')
rdd = rdd.repartition(5)
rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.map(lambda x: (x, 1))
rdd3 = rdd3.coalesce(2)
rdd3.saveAsTextFile('./FileStore/tables/output/2partition')

# COMMAND ----------

rdd = sc.textFile('./FileStore/tables/words')

# COMMAND ----------



# COMMAND ----------


rdd = sc.textFile('./FileStore/tables/output/2partition')
rdd.getNumPartitions()

# COMMAND ----------

rdd.collect()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

rdd2.getNumPartitions()

# COMMAND ----------

rdd.collect()

# COMMAND ----------

rdd2.collect()

# COMMAND ----------

rdd3 = rdd2.repartition(2)

# COMMAND ----------

rdd3.getNumPartitions()

# COMMAND ----------

rdd4 = rdd2.coalesce(2)

# COMMAND ----------

rdd4.collect()

# COMMAND ----------

rdd5 = rdd2.coalesce(10)
