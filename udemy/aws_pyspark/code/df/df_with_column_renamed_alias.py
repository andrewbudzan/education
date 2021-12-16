#!/usr/bin/env python
# coding: utf-8

# In[3]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName('Spark DF').getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
spark.conf.set('spark.sql.repl.eagerEval.maxNumRows', 10)

fp = '../FileStore/tables/StudentData.csv'


# In[9]:


df = spark.read.options(header=True, inferSchema=True).csv(fp)
df.show(5)


# In[10]:


df.withColumnRenamed("gender", "sex").withColumnRenamed("roll", "roll_number").show()


# In[18]:


df2 = df.withColumnRenamed("gender", "sex").withColumnRenamed("name", "full_name")


# In[45]:


df.withColumn('mrks2', ((col('marks') * 0.6 / 5) - col('marks')))


# In[51]:


df.select(col('name').alias('FULL_NAME'), (col('age') * 2).alias("AGE"), col('gender').alias('GENDER'))


# In[ ]:




