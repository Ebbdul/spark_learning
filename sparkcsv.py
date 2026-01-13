from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import SparkConf


spark = SparkSession.builder.appName('WriteDB').getOrCreate()
conf = SparkConf().setAll(pairs=[("spark.jars","C:/Spark/jars/postgresql-42.5.2"), ("spark.jars.packages","org.postgresql:postgresql-42.5.2")])
df = spark.read.csv('employees.csv', header=True, inferSchema=True)
print(df.show()) # print dataframe
# print(df.columns) # column name
# print(df.schema) # show datatypes
# print(df.select('EMPLOYEE_ID', 'FIRST_NAME').groupby('EMPLOYEE_ID', 'FIRST_NAME').count().show())
# print(df.filter(df.EMPLOYEE_ID > 121).show())
# print(df.filter(df['EMPLOYEE_ID']>200).limit(5).show())
# print(df.na.drop().show()) #drop null values
# df2 = df.dropDuplicates()
# print(df2.show())
# df.createOrReplaceTempView('emp')
# sqlquery=spark.sql('select EMPLOYEE_ID, FIRST_NAME, LAST_NAME from emp where EMPLOYEE_ID > 200 group by EMPLOYEE_ID, FIRST_NAME, LAST_NAME order by EMPLOYEE_ID  limit 5')
# print(sqlquery.show())
# print(df.summary().show())
# print(df.take(2))
# print(df.toPandas())
# print(df.explain(mode="formatted"))
# print(df.rdd)
# print(df.na)
# print(df.describe(['EMPLOYEE_ID']).show())


# writing to database
url = "jdbc:postgresql://host:port/database"
table="employeeData"
driver="org.postgresql.Driver"
user="user"
password="password"
# add   .mode("append") \ if you want to write in existing table
df.write.format('jdbc').option("driver", driver).option("url",url).option("dbtable",table).option("user",user).option("password",password).save()

