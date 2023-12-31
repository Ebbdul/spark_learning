
# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()

# Create RDD from parallelize
# dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
# rdd=spark.sparkContext.parallelize(dataList)
# print(rdd)


# Create RDD from external Data source
rdd2 = spark.sparkContext.textFile("example.txt")
print(rdd2)