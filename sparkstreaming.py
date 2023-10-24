from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField,IntegerType, StringType, DateType, DoubleType


# spark= SparkSession.builder.master("local[3]").appName("SparkByExamples").getOrCreate()
spark=SparkSession.builder \
    .appName("Spark NLP")\
    .master("local[4]")\
    .getOrCreate()
schema = StructType([StructField("RecordNumber", IntegerType(), True),
        StructField("Zipcode", StringType(), True),
        StructField("ZipCodeType", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("LocationType", StringType(), True),
        StructField("Lat", StringType(), True),
        StructField("Long", StringType(), True),
        StructField("Xaxis", StringType(), True),
        StructField("Yaxis", StringType(), True),
        StructField("Zaxis", StringType(), True),
        StructField("WorldRegion", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("LocationText", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("Decommisioned", StringType(), True)])


# print(spark)
df = spark.readStream.schema(schema).json("C:/Users/abdul.wasay/PycharmProjects/pythonProject/spark/stream/")
print(df.isStreaming)
# print(df.printSchema())
# df.writeStream.format("console").outputMode("append").start().awaitTermination()
# groupDF = df.select("Zipcode").groupBy("Zipcode").count()
# groupDF.writeStream.format("console").outputMode("complete").start().awaitTermination()