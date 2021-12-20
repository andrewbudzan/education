#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName('Spark DF').getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
spark.conf.set('spark.sql.repl.eagerEval.maxNumRows', 10)
fp = '../FileStore/tables/StudentData.csv'
df = spark.read.options(header=True, inferSchema=True).csv(fp)


# In[6]:


df.sort('course', "marks")


# In[7]:


df.orderBy('course', "marks")


# In[11]:


df.sort(col('course').desc(), df.marks.desc())


# In[ ]:


df.orderBy(col('marks').desc(), df.marks.desc())


# In[ ]:





# In[ ]:





# In[33]:


get_ipython().system('jupyter nbconvert --to script df_count_distinct_duplicate.ipynb')

