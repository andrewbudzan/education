# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('spark DataFrame').getOrCreate()  # ..('name').create() also can be used, but may raise an exeption if already created
filepath = '/FileStore/tables/StudentData.csv'

# COMMAND ----------

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('RDD')
sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile(filepath)
headers = rdd.first()
rdd = rdd.filter(lambda x: x != headers).map(lambda x: x.split(','))

# COMMAND ----------

columns = headers.split(',')
df_rdd = rdd.toDF(columns)
# df_rdd.show()
df_rdd.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField('age', IntegerType(), nullable=True),
    StructField('gender', StringType(), nullable=True),
    StructField('name', StringType(), nullable=True),
    StructField('course', StringType(), nullable=True),
    StructField('roll', StringType(), nullable=True),
    StructField('marks', IntegerType(), nullable=True),
    StructField('email', StringType(), nullable=True)
])

# COMMAND ----------

df_rdd2 = spark.createDataFrame(data=rdd, schema=schema)
df_rdd.show()
# df_rdd2.printSchema()

# COMMAND ----------


