#!/usr/bin/env python
# coding: utf-8

# In[12]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName('Spark DF').getOrCreate()
# spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
# spark.conf.set('spark.sql.repl.eagerEval.maxNumRows', 10)
fp = '../FileStore/tables/StudentData.csv'
df = spark.read.options(header=True, inferSchema=True).csv(fp)


# ### tasks:
# 1. read StudentData.csv into DF
# 2. create a new column 'total_marks' and let it be 120 
# 3. create column 'average' = ('marks'/'total_marks') * 100
# 4. create DF with students who has 'average' > 80 % on the OOP course
# 5. create DF with students who has 'average' > 60 % on the Cloud course
# 6. print names and marks from previous DFs

# In[19]:


# tasks 1, 2, 3

df = (
    df
      .withColumn('total_marks', lit(120))
      .withColumn('average', ((col('marks')/col('total_marks')) * 100))
)


# In[22]:


df.show(5)


# In[29]:


df_oop = df.filter((col('average') > 80) & (col('course') == 'OOP'))


# In[30]:


df_cloud = df.filter((col('average') > 60) & (col('course') == 'Cloud'))


# In[43]:


df_oop.select('name', 'marks').show(5)


# In[44]:


df_cloud.select('name', 'marks').show(5)

