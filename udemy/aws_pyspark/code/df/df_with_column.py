#!/usr/bin/env python
# coding: utf-8

# In[6]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark DF').getOrCreate()
fp = '../FileStore/tables/StudentData.csv'
df = spark.read.options(header=True, inferSchema=True).csv(fp)
df.show()


# In[7]:


df.printSchema()


# In[17]:


from pyspark.sql.functions import col
df = df.withColumn('roll', col('roll').cast("String"))


# In[18]:


df.show()


# ### explanation
# 
# Example 1:
# ```python
# df.withColumn('marks', col('marks') + 1) 
# ```
# how code above works:
#     1. takes columnn 'marks' from dataframe
#     2. increases it's value by 1
#     3. puts result into column 'marks'
#     
# Example 2:
# ```python
# df.withColumn('marks', col('age') * 2) 
# ```
# how code above works:
#     1. takes columnn 'age' from dataframe
#     2. multiplies it's value by 2
#     3. puts result into column 'marks'
# 
#     

# In[40]:


df.withColumn('roll', col('roll').cast("String")).show()


# In[43]:


df.withColumn('marks_x2', col('marks') * 2 * 10).show()


# In[48]:


from pyspark.sql.functions import lit

df.withColumn('Country', lit('usa')).show()


# In[50]:


df.withColumn('roll', col('roll').cast("String")).withColumn('country', lit('usa')).withColumn('marks_x2', col('marks') * 2 * 10).show()


# In[ ]:





# In[65]:


from pyspark.sql.types import StringType
from pyspark.sql.functions import udf


upper_udf = udf(lambda x: str.upper(x)[::-2], StringType())


df.withColumn('name_trnsf', upper_udf('name')).show()


# In[ ]:




