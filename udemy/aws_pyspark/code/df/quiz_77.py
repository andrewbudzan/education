#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName('Spark DF').getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
spark.conf.set('spark.sql.repl.eagerEval.maxNumRows', 10)
fp = '../FileStore/tables/OfficeData.csv'
df = spark.read.options(header=True, inferSchema=True).csv(fp)


# In[4]:


df.sort(df.bonus.asc())


# In[5]:


df.sort(col("age").desc(), col('salary').asc())


# In[6]:


df.sort(col("age").desc(), col('bonus').desc(), col('salary').asc())


# In[ ]:





# In[12]:


df.orderBy(col('marks').desc(), df.course.desc())


# In[ ]:





# In[ ]:





# In[13]:


get_ipython().system('jupyter nbconvert --to script df_sort_orderby.ipynb')

