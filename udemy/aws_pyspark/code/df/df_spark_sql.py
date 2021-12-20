#!/usr/bin/env python
# coding: utf-8

# In[3]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName('Spark DF').getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True) 
spark.conf.set('spark.sql.repl.eagerEval.maxNumRows', 10)
fp = '../FileStore/tables/StudentData.csv'
df = spark.read.options(header=True, inferSchema=True).csv(fp)

# df.createOrReplaceTempView('student')


# # same result
# df.select('*').show()
# spark.sql('select * from student').show()
#
#
# # In[15]:
#
#
# # same result
# df.groupby('course', 'gender').count().show()
# spark.sql('select course, gender, count(*) as count from student group by course, gender').show()



fw = './output'
df.write.options(header=True, delimiter=',').csv(fw)
