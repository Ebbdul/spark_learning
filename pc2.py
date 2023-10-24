import xml.etree.ElementTree as ET
import pandas as pd
from pyspark.sql import SparkSession
import os
import sys
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField,IntegerType, StringType,DateType, DoubleType
from pyspark.sql.functions import concat_ws, col, monotonically_increasing_id
import warnings

# Ignore all warnings
warnings.filterwarnings("ignore")
warnings.filterwarnings("ignore", category=DeprecationWarning)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.13.0").getOrCreate()\
    # .config("spark.jars.packages","com.microsoft.azure:spark-mssql-connector_2.12:1.2.0")\
conf = SparkConf().set("park.driver.extraClassPath","C:/Spark/jars/mssql-jdbc-7.2.2.jre8").setAll(pairs=[("spark.jars","C:/Spark/jars/spark-mssql-connector_2.12-1.2.0,C:/Spark/jars/sqlserverjdbc"), ("spark.jars.packages","com.microsoft.azure:spark-mssql-connector_2.12:1.2.0")])

try:
    tree = ET.parse('samplexml500.xml')
    root = tree.getroot()

    store_items = []
    all_items = []
    like_items = []
    all_like_items = []
    multi_items = []
    all_multi_items = []

    for child in root.findall('feeds'):
        if child.find('id') is not None and child.find('title') is not None and child.find('description') is not None \
                and child.find('location') is not None and child.find('lng') is not None and child.find(
            'lat') is not None \
                and child.find('userId') is not None and child.find('name') is not None and child.find(
            'isdeleted') is not None \
                and child.find('profilePicture') is not None and child.find('mediatype') is not None and child.find(
            'commentCount') is not None \
                and child.find('createdAt') is not None and child.find('code') is not None:
            pid = child.find('id').text
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
            store_items = [pid, title, description, location, lng, lat, userId, name, isdeleted, profilePicture,
                           mediatype,
                           commentCount, createdAt, code]
            all_items.append(store_items)

            for subchild in child.findall('likeDislike'):
                likes = subchild.find('likes').text
                dislikes = subchild.find('dislikes').text
                userAction = subchild.find('userAction').text
                like_items = [pid, likes, dislikes, userAction]
                all_like_items.append(like_items)

            for schild in child.findall('multiMedia'):
                id = schild.find('id').text
                name = schild.find('name').text
                description = schild.find('description').text
                url = schild.find('url').text
                mediatype = schild.find('mediatype').text
                likeCount = schild.find('likeCount').text
                createAt = schild.find('createAt').text
                multi_items = [pid, id, name, description, url, mediatype, likeCount, createAt]
                all_multi_items.append(multi_items)

    feed_columns = ['pid', 'f_title', 'f_description', 'location', 'lng', 'lat', 'userId', 'f_name', 'isdeleted',
                    'profilePicture',
                    'f_mediatype', 'commentCount', 'createdAt', 'code']
    feeds_df = pd.DataFrame(all_items, columns=feed_columns)
    spark_feeds_df = spark.createDataFrame(feeds_df)  # create the spark dataframe from pandas

    like_columns = ['pid', 'likes', 'dislikes', 'userAction']
    likes_df = pd.DataFrame(all_like_items, columns=like_columns)
    spark_like_df = spark.createDataFrame(likes_df)  # created the spark df from pandas df
    schema = StructType([StructField('pid', StringType(), True),
                         StructField('m_id', StringType(), True),
                         StructField('m_name', StringType(), True),
                         StructField('m_description', StringType(), True),
                         StructField('url', StringType(), True),
                         StructField('m_mediatype', StringType(), True),
                         StructField('likeCount', StringType(), True),
                         StructField('createAt', StringType(), True)])

    multi_columns = ['pid', 'm_id', 'm_name', 'm_description', 'url', 'm_mediatype', 'likeCount', 'createAt']
    multi_df = pd.DataFrame(all_multi_items, columns=multi_columns)
    spark_multi_df = spark.createDataFrame(multi_df, schema=schema)  # created the spark df from pandas df

    # joining feeds df and multi df via spark using outer join
    spark_merged_df = spark_feeds_df.join(spark_multi_df, 'pid', 'inner')
    # now joining above spark_merged_df with spark like df
    spark_final_df = spark_merged_df.join(spark_like_df, 'pid', 'inner')
    # print(spark_final_df.show(n=1000))


except Exception as e:
    print("An error occurred:", str(e))



# now loading the data into sql

def load(n,t):
    try:
        n.write \
            .format("jdbc") \
            .mode("overwrite").option("url", "jdbc:sqlserver://192.168.2.49;databaseName=test-db-wasay;") \
            .option("dbtable", t) \
            .option("user", "sa") \
            .option("password", "Red*St0ne") \
            .save()

    except Exception as e:
        print("Error occur in loading data into Database:", str(e))

load(spark_final_df, 'spark_final_df_500')


