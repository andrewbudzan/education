# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('Students Project')
sc = SparkContext.getOrCreate(conf=conf)

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

def rdd_mapper(x):
    data = dict(zip(headers.split(','), x.split(',')))
    data['age'] = int(data.get("age", 0))
    data['marks'] = int(data.get('marks', 0))
    return list(data.values())

# COMMAND ----------

headers

# COMMAND ----------

rdd = rdd.map(rdd_mapper)
rdd.collect()

# COMMAND ----------

# 1. number of students
rdd.count()

# COMMAND ----------

# 2. total marks achieved by Female and Male students
rdd.map(lambda x: (x[1], x[-2])).groupByKey().mapValues(sum).collect()

# COMMAND ----------

# 3. total number of students that have passed and failed. 50+ required to pass

passed = rdd.filter(lambda x: x[-2] > 50).count()

failed = rdd.filter(lambda x: x[-2] <= 50).count()

print(f'passed: {passed}\nfailed: {failed}')


# COMMAND ----------

# 4. total number of students enrolled per cource

rdd.map(lambda x: (x[3], 1)).groupByKey().mapValues(sum).collect()

# rdd.map(lambda x: (x[3], 1)).reduceByKey(lambda x, y: x + y).collect()

# COMMAND ----------

# 5. total marks that students achieved per cource

l1 = rdd.map(lambda x: (x[3], x[-2])).groupByKey().mapValues(sum).collect()

l2 = rdd.map(lambda x: (x[3], x[-2])).reduceByKey(lambda x, y: x + y).collect()

sorted(l1) == sorted(l2)

# COMMAND ----------

# 6. average marks that students achieved per cource

res_grouped = rdd.map(lambda x: (x[3], x[-2])).groupByKey().mapValues(lambda x: sum(x)/len(x)).collect()

res_long = rdd.map(lambda x: (x[3], (int(x[5]), 1) )).reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])).map(lambda x: (x[0], (x[1][0] / x[1][1]))).collect()

res_map_val = rdd.map(lambda x: (x[3], (int(x[5]), 1) )).reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])).mapValues(lambda x: (x[0] / x[1])).collect()

print(res_grouped, '\n', res_map_val, '\n', res_long)

sorted(res_grouped) == sorted(res_long) == sorted(res_map_val)

# COMMAND ----------

# 7. min and max marks that students achieved per cource

print(rdd.map(lambda x: (x[3], x[-2])).groupByKey().mapValues(lambda x: f'min mark: {min(list(x))}; max mark: {max(list(x))}').collect())

print(rdd.map(lambda x: (x[3], x[-2])).reduceByKey(lambda x,y: x if x > y else y).collect())
print(rdd.map(lambda x: (x[3], x[-2])).reduceByKey(lambda x,y: x if x < y else y).collect())

# COMMAND ----------

# 8. average age of Male and Female students

rdd.map(lambda x: (x[1], x[0])).groupByKey().mapValues(lambda x: round(sum(x)/len(x), ndigits=2)).collect()
