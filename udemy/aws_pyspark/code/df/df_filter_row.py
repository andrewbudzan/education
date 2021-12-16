#!/usr/bin/env python
# coding: utf-8

# In[3]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName('Spark DF').getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
spark.conf.set('spark.sql.repl.eagerEval.maxNumRows', 10)
fp = './FileStore/tables/StudentData.csv'
df = spark.read.options(header=True, inferSchema=True).csv(fp)


# In[5]:


df


# In[7]:


df.filter(df.course == 'DB')


# In[10]:


df.filter(col('course') == 'DB')


# In[14]:


df.filter((df.course == 'DB') & (df.marks > 50)).show()


# In[16]:


courses = ['DB', 'Cloud', 'OOP', 'DSA']
df.filter(df.course.isin(courses)).show()


# In[17]:


df.filter(df.course.startswith('D')).show()


# In[24]:


df.filter(df.name.endswith('se')).show()


# In[26]:


df.filter(df.name.contains('is')).show()


# In[30]:


df.filter(df.name.like('%s%e%')).show()


# In[ ]:


from collections import namedtuple
namedtuple()
