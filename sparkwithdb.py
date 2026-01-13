from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import SparkConf


conf = SparkConf().setAll(pairs=[("spark.jars","C:/Spark/jars/postgresql-42.5.2"), ("spark.jars.packages","org.postgresql:postgresql-42.5.2")])
spark = SparkSession.builder.appName('readDB').getOrCreate()
url = "jdbc:postgresql://hsot:port/database"
table='"EDW".booking_fact'
driver="org.postgresql.Driver"
user="postgres"
password="Red*St0ne"
query='select count(1), nbook_id_bk from "EDW".booking_fact group by nbook_id_bk'


df = spark.read.format('jdbc').option("driver", driver).option("url",url).option("dbtable",table).option("user",user).option("password",password).load()
# print(df.show())
# Display database
# table schema
#
df.createOrReplaceTempView('hms_edw')
df2=spark.sql('select count(1), nbook_id_bk from hms_edw group by nbook_id_bk')
print(df2.show())
