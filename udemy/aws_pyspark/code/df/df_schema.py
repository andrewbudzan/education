# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('spark DataFrame').getOrCreate()  # ..('name').create() also can be used, but may raise an exeption if already created
filepath = '/FileStore/tables/StudentData.csv'
df = spark.read.csv(path=filepath, sep=',', header=True)
# df = spark.read.option("header", True).csv(path=filepath, sep=',')
df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# infer schema
df_infer_schema = spark.read.options(
    header=True,
    inferSchema=True,
    delimiter=','
).csv(path=filepath)

df_infer_schema.show()

# COMMAND ----------

# provided schema
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

df_provided_schema = spark.read.options(
    header=True,
).schema(schema=schema).csv(path=filepath)

df_provided_schema.printSchema()

# COMMAND ----------
