# author: andrii budzan
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark DF').getOrCreate()
fp = './FileStore/tables/StudentData.csv'
df = spark.read.options(header=True, inferSchema=True).csv(fp)
df.show()

df.select('name', 'gender').show()


df.select(df.name, df.email).show()

if __name__ == '__main__':
    pass
