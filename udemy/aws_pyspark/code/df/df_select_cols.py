#!/usr/bin/env python
# coding: utf-8

# In[4]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark DF').getOrCreate()
fp = '../FileStore/tables/StudentData.csv'


# In[5]:


df = spark.read.options(header=True, inferSchema=True).csv(fp)


# In[6]:


df.show()


# In[41]:


from pyspark.sql.functions import col

# all results are the same:
df.select('name', 'age', 'gender').show()
# df.select(df.name, df.age, df.gender).show()
# df.select(col('name'), col('age'), col('gender')).show()


# In[43]:


# same to sql
df.select('*').show()


# In[52]:


df.columns[2]

df.select(df.columns[2:6]).show()


# In[56]:


df.select(['age', col("name"), *df.columns[4:]]).show()


# In[57]:


df2 = df.select(['age', col("name"), *df.columns[4:]])


# In[67]:


df2.show()


# In[65]:


df2.head(5)


# In[66]:


df2.tail(5)


# In[ ]:


get_ipython().run_line_magic('pip', 'insta')


# In[68]:


get_ipython().system('jupyter nbconvert --to script df_select_cols.ipynb')


# In[ ]:





# In[ ]:
