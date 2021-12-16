# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('MinMax')
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/movie_ratings.csv')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.map(lambda x: (x.split(',')[0], int(x.split(',')[1])))
rdd2.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC  ('12 Angry Men', 3)  
# MAGIC  ('12 Angry Men', 4)  
# MAGIC  ('12 Angry Men', 2)  
# MAGIC  ('12 Angry Men', 1)  
# MAGIC  ('12 Angry Men', 1)  
# MAGIC 
# MAGIC we will have 4 iterations of reducing  
# MAGIC 
# MAGIC lambda  x, y :  x if x < y else y  
# MAGIC         3, 4 => 3 - first element rating is 3, and second is 4. according to our lambda func if x < y, current x stays as x for the next iteration
# MAGIC         3, 2 => 2
# MAGIC         2, 1 => 1
# MAGIC         1, 1 => 1

# COMMAND ----------

rdd2.reduceByKey(lambda x, y: x if x > y else y).collect()

# COMMAND ----------

rdd2.reduceByKey(lambda x, y: x if x < y else y).collect()

# COMMAND ----------



# COMMAND ----------


