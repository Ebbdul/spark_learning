from pyspark.sql.types import *
from pyspark import SparkContext
#creating rdd
spark = SparkContext.getOrCreate()
data = ([1,2,3,4,5])

rdd= spark.parallelize(data)
rdd.saveAsTextFile('testfile.txt')
print(rdd)