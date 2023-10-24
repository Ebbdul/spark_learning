from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.config("spark.some.config.option", "some-value").getOrCreate()
df = spark.read.json('people.json')
# print(df.head(3))
# print(df.schema)    # it will print data type
print(df.show())
# print(df.select("name").show())
# df2 = df.select(df['name'], df['age']  +1)
# print(df2.show())
# print(df.filter(df['age']> 18))
# print(df.groupby('age').count().show())

# in order to run sql query
# df.createOrReplaceTempView('people')
# sqldf= spark.sql('select * from people')
# print(sqldf.show())

#define schema
# peopple = spark.sparkContext.parallelize(df)
# people =spark.parallelize(df)

# schema = StructType([StructField('name', StringType(), True),
#                     StructField('age', StringType(), True)])
# schema_people= spark.createDataFrame('people', schema)
# print(schema_people.show())
# schema_people.createOrReplaceTempView('people')
# query = spark.sql('select name from people')
# print(query.show())