#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName('Spark DF').getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
spark.conf.set('spark.sql.repl.eagerEval.maxNumRows', 10)
fp = '../FileStore/tables/StudentData.csv'
df = spark.read.options(header=True, inferSchema=True).csv(fp)


# In[5]:


df.count()


# In[10]:


df.filter(col('course') == 'DB').count()


# In[11]:


df.distinct().count()


# In[19]:


df.select(col('gender'), col('age')).distinct()


# In[ ]:


df.select(df.age, df.gender,


# In[4]:


df.dropDuplicates(['age', 'gender', 'course']).count()


# In[33]:


get_ipython().system('jupyter nbconvert --to script df_count_distinct_duplicate.ipynb')

