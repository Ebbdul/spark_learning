import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *
# import com.databricks.spark.xml_
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField,IntegerType, StringType, DateType, DoubleType
from pyspark.sql.functions import concat_ws,col
from pyspark.sql.functions import explode
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.13.0").getOrCreate()
    # .config("spark.jars.packages","com.microsoft.azure:spark-mssql-connector_2.12:1.2.0")\
#
#
conf = SparkConf().set("park.driver.extraClassPath","C:/Spark/jars/mssql-jdbc-7.2.2.jre8").setAll(pairs=[("spark.jars","C:/Spark/jars/spark-mssql-connector_2.12-1.2.0,C:/Spark/jars/sqlserverjdbc"), ("spark.jars.packages","com.microsoft.azure:spark-mssql-connector_2.12:1.2.0")])

import xml.etree.ElementTree as ET
from tabulate import tabulate
import pandas as pd

tree = ET.parse('feeds.xml')
root = tree.getroot()
# print(ET.tostring(root, encoding='utf8').decode('utf8'))
# print(root)
feed=[]
likedislike=[]
multimedia=[]
for child in root:
    feeds=child.tag+" "+child.text
    # print(feeds)
    feed.append(feeds)
# print("--------------liskdislike")

for likeDislike in root.find('likeDislike'):
    # print(likeDislike)
    like=likeDislike.tag +" "+ likeDislike.text
    # print(str(like))
    likedislike.append(like)
    # print(list(like))
# print("----------------multiMedia")
for multiMedia in root.find('multiMedia'):
    # print(multiMedia)
    multi=multiMedia.tag +" "+str(multiMedia.text)
    # print(multi)
    multimedia.append(multi)
complete=feed+likedislike+multimedia
# print(likedislike)
collist1=[]
valuelist1=[]
collist2=[]
valuelist2=[]
collist3=[]
valuelist3=[]
for i in likedislike:
    n=i.split(' ')
    collist1.append(n[0])
    valuelist1.append(n[1])
# print(collist1)
# print(valuelist1)
valuelist=[valuelist1]
like_dataframe=pd.DataFrame(valuelist, columns=collist1)
# print(like_dataframe.to_string(index=False))
for i in multimedia:
    n = i.split(' ')
    collist2.append(n[0])
    valuelist2.append(n[1])
multivaluelist=[valuelist2]
multimedia_dataframe=pd.DataFrame(multivaluelist, columns=collist2)
# print(multimedia_dataframe.to_string(index=False))
# similarly for feeds
for i in feed:
    n = i.split(' ')
    collist3.append(n[0])
    valuelist3.append(n[1])
feedsvaluelist=[valuelist3]
feeds_dataframe=pd.DataFrame(feedsvaluelist, columns=collist3)
# print(feeds_dataframe.to_string(index=False))
df3=pd.concat([like_dataframe, multimedia_dataframe], axis=1)
# print(df3)
complete_dataframe=pd.concat([feeds_dataframe, df3], axis=1)
print(complete_dataframe)
# complete_dataframe.to_csv('textxmlDATAFRAME.csv')
for i in complete:
    if "likeDislike" in i:
        if "multiMedia" in i:
            continue
        continue
    # print(i)
# for child in root.iter():
    # print(child.tag, child.text)
# print(like_dataframe.to_string(index=False))
# likecol=['likes', 'dislikes','userAction']
sparkDFlike=spark.createDataFrame(like_dataframe)
# print(sparkDFlike.show())
sparkDFmulti=spark.createDataFrame(multimedia_dataframe)
sparkDFcomplete=spark.createDataFrame(complete_dataframe)
print(sparkDFlike.show())
record="samplefeeds"
like="tablelike"
multi="tablemulti"
feedtable="tablefeeds"
# sparkDFcomplete.write \
#   .format("jdbc") \
#   .mode("overwrite").option("url", "jdbc:sqlserver://192.168.2.49;databaseName=test-db-wasay;") \
#   .option("dbtable", feedtable) \
#   .option("user", "sa") \
#   .option("password", "Red*St0ne") \
#   .save()


