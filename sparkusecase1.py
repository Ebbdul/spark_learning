from pyspark.sql import SparkSession
from pyspark.sql.types import *
# import com.databricks.spark.xml_
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField,IntegerType, StringType, DateType, DoubleType
from pyspark.sql.functions import concat_ws,col
from pyspark.sql.functions import explode
spark = SparkSession.builder.config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.13.0").getOrCreate()
    # .config("spark.jars.packages","com.microsoft.azure:spark-mssql-connector_2.12:1.2.0")\


conf = SparkConf().set("park.driver.extraClassPath","C:/Spark/jars/mssql-jdbc-7.2.2.jre8").setAll(pairs=[("spark.jars","C:/Spark/jars/spark-mssql-connector_2.12-1.2.0,C:/Spark/jars/sqlserverjdbc"), ("spark.jars.packages","com.microsoft.azure:spark-mssql-connector_2.12:1.2.0")])
#
# schema = StructType([StructField("Database", StringType(), True),
#                      StructField("ProteinEntry", StringType(), True),
#                      StructField("_date", DateType(), True ),
#                      StructField("_id", StringType(), True),
#                      StructField("_release", StringType(), True),
#                      StructField("link", StringType(), True),
#                      StructField("style", StringType(), True),
#                      StructField("classification", StringType(), True),
#                      StructField("superfamily", StringType(), True),
#                      StructField("feature", StringType(), True),
#                      StructField("_label", StringType(), True),
#                      StructField("description", StringType(), True),
#                      StructField("feature-type", StringType(), True),
#                      StructField("seq-spec", StringType(), True),
#                      StructField("status", StringType(), True),
#                      StructField("genetics", StringType(), True),
#                      StructField("introns", StringType(), True),
#                      StructField("header", StringType(), True),
#                      StructField("accession", StringType(), True),
#                      StructField("created_date", StringType(), True),
#                      StructField("seq-rev_date", StringType(), True),
#                      StructField("txt-rev_date", StringType(), True),
#                      StructField("uid", StringType(), True),
#                      StructField("keywords", StringType(), True),
#                      StructField("keyword", StringType(), True),
#                      StructField("organism", StringType(), True),
#                      StructField("common", StringType(), True),
#                      StructField("formal", StringType(), True),
#                      StructField("source", StringType(), True),
#                      StructField("protein", StringType(), True),
#                      StructField("name", StringType(), True),
#                      StructField("reference", StringType(), True),
#                      StructField("accinfo", StringType(), True),
#                      StructField("exp-source", StringType(), True),
#                      StructField("mol-type", StringType(), True),
#                      StructField("xrefs", StringType(), True),
#                      StructField("db", StringType(), True),
#                      StructField("contents", StringType(), True),
#                      StructField("note", StringType(), True),
#                      StructField("refinfo", StringType(), True),
#                      StructField("_refid", StringType(), True),
#                      StructField("authors", StringType(), True),
#                      StructField("author", StringType(), True),
#                      StructField("citation", StringType(), True),
#                      StructField("_VALUE", StringType(), True),
#                      StructField("_type", StringType(), True),
#                      StructField("month", StringType(), True),
#                      StructField("pages", StringType(), True),
#                      StructField("title", StringType(), True),
#                      StructField("volume", StringType(), True),
#                      StructField("xml_repository", StringType(), True),
#                      StructField("xref", StringType(), True),
#                      StructField("year", StringType(), True),
#                      StructField("sequence", StringType(), True),
#                      StructField("summary", StringType(), True),
#                      StructField("length", DoubleType(), True),
#                      StructField("type", StringType(), True),
#                      StructField("_rel", StringType(), True),
#                      StructField("_lang", StringType(), True)])
# schema=StructType([StructField('code', LongType(), True), StructField('commentCount', LongType(), True), StructField('createdAt', TimestampType(), True), StructField('description', StringType(), True), StructField('feedsComment', StringType(), True), StructField('id', LongType(), True), StructField('imagePaths', StringType(), True), StructField('images', StringType(), True), StructField('isdeleted', BooleanType(), True), StructField('lat', LongType(), True), StructField('likeDislike', StructType([StructField('dislikes', LongType(), True), StructField('likes', LongType(), True), StructField('userAction', LongType(), True)]), True), StructField('lng', LongType(), True), StructField('location', StringType(), True), StructField('mediatype', LongType(), True), StructField('msg', StringType(), True), StructField('multiMedia', ArrayType(StructType([StructField('createAt', TimestampType(), True), StructField('description', StringType(), True), StructField('id', LongType(), True), StructField('likeCount', LongType(), True), StructField('mediatype', LongType(), True), StructField('name', StringType(), True), StructField('place', StringType(), True), StructField('url', StringType(), True)]), True), True), StructField('name', StringType(), True), StructField('profilePicture', StringType(), True), StructField('title', StringType(), True), StructField('userId', LongType(), True), StructField('videoUrl', StringType(), True)])
schema=StructType([StructField('code', LongType(), True), StructField('commentCount', LongType(), True), StructField('createdAt', TimestampType(), True), StructField('description', StringType(), True), StructField('feedsComment', StringType(), True), StructField('id', LongType(), True), StructField('imagePaths', StringType(), True), StructField('images', StringType(), True), StructField('isdeleted', BooleanType(), True), StructField('lat', LongType(), True), StructField('likeDislike', StructType([StructField('dislikes', LongType(), True), StructField('likes', LongType(), True), StructField('userAction', LongType(), True)]), True), StructField('lng', LongType(), True), StructField('location', StringType(), True), StructField('mediatype', LongType(), True), StructField('msg', StringType(), True), StructField('multiMedia', ArrayType(StructType([StructField('createAt', TimestampType(), True), StructField('description', StringType(), True), StructField('id', LongType(), True), StructField('likeCount', LongType(), True), StructField('mediatype', LongType(), True), StructField('name', StringType(), True), StructField('place', StringType(), True), StructField('url', StringType(), True)]), True), True), StructField('name', StringType(), True), StructField('profilePicture', StringType(), True), StructField('title', StringType(), True), StructField('userId', LongType(), True), StructField('videoUrl', StringType(), True)])

df = spark.read.format('com.databricks.spark.xml').option("rootTag","root").option("rowTag","feeds").schema(schema).load("Samplexml500.xml")
# df = spark.read.format('com.databricks.spark.xml').option("rowTag","record").load("tableConvert.com_9ken2s.xml")
# df = spark.read.format('com.databricks.spark.xml').option("rowTag","book").load("books.xml")
# print(df.columns)
print(df.show())
# a=df.withColumn("likeDislike",df.likeDislike.cast(StringType())).withColumn("multiMedia", df.multiMedia.cast(StringType()))
# a.printSchema()
# print(a.show())
# df.printSchema()
# print(df.select("likeDislike.*").show())
# print(df.select("multiMedia.likecount").show())
# df2=df.withColumn("multiMedia",concat_ws(",",col("multiMedia")))
# print(df2.show())




#write to sql server
  # .option("driver","ODBC Driver 18 for SQL Server")

# .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver") \
tabeltest="xmltable"
tablexml="proteinxml"
tablebook="bookxml"
iris="irisdata"
record="record500"
