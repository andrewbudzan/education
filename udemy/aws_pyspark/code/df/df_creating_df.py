# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('spark DataFrame').getOrCreate()  # ..('name').create() also can be used, but may raise an exeption if already created
filepath = '/FileStore/tables/StudentData.csv'
df = spark.read.csv(path=filepath, sep=',', header=True)
# df = spark.read.option("header", True).csv(path=filepath, sep=',')
df.show()
