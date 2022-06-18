from pyspark.sql.functions import expr

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

sparkConf = SparkConf()
sparkConf.set("spark.app.name", 'flatmapExample')
sparkConf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

data=[("James","Bond"),("Scott","Varsa")]
df=spark.createDataFrame(data).toDF("col1","col2")
df.withColumn("Name",expr(" col1 ||','|| col2")).show()


data = [("James","M"),("Michael","F"),("Jen","")]
columns = ["name","gender"]
df = spark.createDataFrame(data = data, schema = columns)


df2=df.withColumn("gender", expr("CASE WHEN gender = 'M' THEN 'Male' " +
                                 "WHEN gender = 'F' THEN 'Female' ELSE 'unknown' END"))
df2.show()

data=[("2019-01-23",1),("2019-06-24",2),("2019-09-20",3)]
df=spark.createDataFrame(data).toDF("date","increment")

df.select(df.date, df.increment, expr("add_months(date,increment)").alias("inc_date")).show()

df.select(df.date,df.increment,expr("increment + 5 as new_increment")).show()

data=[(100,2),(200,3000),(500,500)]
df=spark.createDataFrame(data).toDF("col1","col2")
df.filter(expr("col1 == col2")).show()