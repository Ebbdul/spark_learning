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
    # .config("spark.jars.packages","com.microsoft.azure:spark-mssql-connector_2.12:1.2.0")\
conf = SparkConf().set("park.driver.extraClassPath","C:/Spark/jars/mssql-jdbc-7.2.2.jre8").setAll(pairs=[("spark.jars","C:/Spark/jars/spark-mssql-connector_2.12-1.2.0,C:/Spark/jars/sqlserverjdbc"), ("spark.jars.packages","com.microsoft.azure:spark-mssql-connector_2.12:1.2.0")])

tree = ET.parse('20xml.xml')
root = tree.getroot()
# print(root)
# feeds=[]
# feedscol=[]
# feedsvalue=[]
# like=[]
# likecol=[]
# likevalue=[]
# multi=[]
# multicol=[]
# multivalue=[]
store_items = []
all_items = []
likeitem=[]
all_like_item=[]
multi_item=[]
all_multi_item=[]
for child in root.findall('feeds'):
    # print(child.tag, child.text)
    # print(child)
    if child.find('id').text is not None and child.find('title').text is not None and child.find('description').text is not None\
            and child.find('location').text is not None and child.find('lng').text is not None and child.find('lat').text is not None\
            and child.find('userId').text is not None and child.find('name').text is not None and child.find('isdeleted').text is not None \
            and child.find('profilePicture').text is not None and child.find(
        'mediatype').text is not None and child.find('commentCount').text is not None \
            and child.find('createdAt').text is not None and child.find('code').text is not None:
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
            likes=subchild.find('likes').text
            dislikes=subchild.find('dislikes').text
            userAction=subchild.find('userAction').text
            likeItem=[likes, dislikes, userAction]
            all_like_item.append(likeItem)
        for schild in child.findall('multiMedia'):
            id=schild.find('id').text
            name=schild.find('name').text
            description=schild.find('description').text
            url=schild.find('url').text
            mediatype=schild.find('mediatype').text
            likeCount=schild.find('likeCount').text
            createAt=schild.find('createAt').text
            multi_item=[id, name, description, url, mediatype, likeCount, createAt]
            all_multi_item.append(multi_item)


feedscolumns=[ 'id','title', 'description', 'location', 'lng', 'lat', 'userId', 'name', 'isdeleted', 'profilePicture', 'mediatype', 'commentCount', 'createdAt','code']
feedsdf=spark.createDataFrame(all_items, schema=feedscolumns)
# print(feedsdf.show())
like_col=['likes','dislikes','userAction']
likedf=spark.createDataFrame(all_like_item, schema=like_col)
# print(likedf.show())
multi_col=['id', 'name', 'description', 'url', 'mediatype', 'likeCount', 'createAt']
schema = StructType([StructField('id', StringType(), True),
                    StructField('name', StringType(), True),
                     StructField('description', StringType(), True),
                     StructField('url', StringType(), True),
                     StructField('mediatype', StringType(), True),
                     StructField('likeCount', StringType(), True),
                     StructField('createAt', StringType(), True)])
multi_dataframe=spark.createDataFrame(all_multi_item, schema=schema)
# print(multi_dataframe.show())
multi_dataframe=multi_dataframe.withColumn("id1",monotonically_increasing_id())
likedf=likedf.withColumn("id2",monotonically_increasing_id())
merged_dataframe=multi_dataframe.join(likedf, col("id1")==col("id2"), "full" ).drop("id1","id2")
# print(merged_dataframe.show(n=700))
merged_dataframe=merged_dataframe.na.fill("0", ["likes"]).na.fill("1", ["dislikes"]).na.fill("2", "userAction")
print(merged_dataframe.show(n=1000))
