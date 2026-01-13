import xml.etree.ElementTree as ET
import pandas as pd
from pyspark.sql import SparkSession
import os
import sys
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField,IntegerType, StringType, DateType, DoubleType
from pyspark.sql.functions import concat_ws, col, monotonically_increasing_id
from pyspark.sql.functions import lit
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.13.0").getOrCreate()\
#     # .config("spark.jars.packages","com.microsoft.azure:spark-mssql-connector_2.12:1.2.0")\
# # #
# #
conf = SparkConf().set("park.driver.extraClassPath","C:/Spark/jars/mssql-jdbc-7.2.2.jre8").setAll(pairs=[("spark.jars","C:/Spark/jars/spark-mssql-connector_2.12-1.2.0,C:/Spark/jars/sqlserverjdbc"), ("spark.jars.packages","com.microsoft.azure:spark-mssql-connector_2.12:1.2.0")])

tree = ET.parse('20xml.xml')
root = tree.getroot()
# print(root)
store_items=[]
all_items=[]
all_multi_item=[]
all_like_item=[]
for child in root.findall('feeds'):
    id = child.find('id').text
    title = child.find('title').text
    description = child.find('description').text
    location = child.find('location').text
    lng = child.find('lng').text
    lat = child.find('lat').text
    userId = child.find('userId').text
    name = child.find('name').text
    isdeleted = child.find('isdeleted').text
    profilePicture = child.find('profilePicture').text
    mediatype = child.find('mediatype').text
    commentCount = child.find('commentCount').text
    createdAt = child.find('createdAt').text
    code = child.find('code').text
    store_items = [id, title, description, location, lng, lat, userId, name, isdeleted, profilePicture, mediatype,
                   commentCount, createdAt, code]
    all_items.append(store_items)
    for subchild in child.findall('likeDislike'):
        likes = subchild.find('likes').text
        dislikes = subchild.find('dislikes').text
        userAction = subchild.find('userAction').text
        likeItem = [likes, dislikes, userAction]
        all_like_item.append(likeItem)
    for schild in child.findall('multiMedia'):
        id = schild.find('id').text
        name = schild.find('name').text
        description = schild.find('description').text
        url = schild.find('url').text
        mediatype = schild.find('mediatype').text
        likeCount = schild.find('likeCount').text
        createAt = schild.find('createAt').text
        multi_item = [id, name, description, url, mediatype, likeCount, createAt]
        all_multi_item.append(multi_item)
feedscolumns=[ 'id','title', 'description', 'location', 'lng', 'lat', 'userId', 'name', 'isdeleted', 'profilePicture', 'mediatype', 'commentCount', 'createdAt','code']
feedsdf=pd.DataFrame(all_items, columns=feedscolumns)
# print(feedsdf)
feedsdf['feeds_index']=feedsdf.index
# print(feedsdf)
sparkfeedsDF=spark.createDataFrame(feedsdf)
sparkfeedsDF=sparkfeedsDF.withColumnRenamed('id', 'id_feeds')
sparkfeedsDF=sparkfeedsDF.withColumnRenamed('description', 'feeds_description')
sparkfeedsDF=sparkfeedsDF.withColumnRenamed('name','feeds_name')
sparkfeedsDF=sparkfeedsDF.withColumnRenamed('mediatype', 'feeds_mediatype')
print(sparkfeedsDF.show(n=1000))
# count=feedsdf.count()
# print(count)
# i=0
# for i in range(count):
#     i=i+1
#     # print(i)
# feedsdf=feedsdf.withColumn("feeds_id", lit(str(i)))
# print(feedsdf.show())


# print(feedsdf.show())
like_col=['likes','dislikes','userAction']
likedf=pd.DataFrame(all_like_item, columns=like_col)
likedf['like_index']=likedf.index
sparklikedf=spark.createDataFrame(likedf)
# print(sparklikedf.show())
# likedf=likedf.withColumn("like_id", monotonically_increasing_id())
# print(likedf.show())
multi_col=['id', 'name', 'description', 'url', 'mediatype', 'likeCount', 'createAt']
schema = StructType([StructField('id', StringType(), True),
                    StructField('name', StringType(), True),
                     StructField('description', StringType(), True),
                     StructField('url', StringType(), True),
                     StructField('mediatype', StringType(), True),
                     StructField('likeCount', StringType(), True),
                     StructField('createAt', StringType(), True),
                     StructField('multi_index', IntegerType(), True)])
multi_dataframe=pd.DataFrame(all_multi_item, columns=multi_col)
multi_dataframe['multi_index']=multi_dataframe.index
sparkmultiDF=spark.createDataFrame(multi_dataframe, schema=schema)
sparkmultiDF=sparkmultiDF.withColumnRenamed('id', 'id_multi')
sparkmultiDF=sparkmultiDF.withColumnRenamed('description', 'multi_description')
sparkmultiDF=sparkmultiDF.withColumnRenamed('name', 'multi_name')
# print(sparkmultiDF.show())
# multi_dataframe=multi_dataframe.withColumn("multi_id", monotonically_increasing_id())

# merged_dataframe=sparkfeedsDF.join(sparkmultiDF, sparkfeedsDF["feeds_index"]==sparkmultiDF["multi_index"]).join(sparklikedf, sparkmultiDF["multi_index"]==sparklikedf['like_index'])
# print(merged_dataframe.show(n=1000))
sparkmultiDF.write \
  .format("jdbc") \
  .mode("overwrite").option("url", "jdbc:sqlserver://host;databaseName=databaseName;") \
  .option("dbtable", 'spark_2dmulti') \
  .option("user", "user") \
  .option("password", "password") \
  .save()
