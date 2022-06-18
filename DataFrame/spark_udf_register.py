#Creating and registering the udf in spark
from pyspark.sql.types import StringType

from pyspark.sql.functions import udf, expr

from pyspark import SparkConf
from pyspark.sql import SparkSession

#creating SparkSession
sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "udf_register")

spark = SparkSession.builder.config(conf = sparkConf).enableHiveSupport().getOrCreate()

#setted Log Lelel to ERRoR
spark.sparkContext.setLogLevel("ERROR")

def age_check(age):
    if age > 18:
        return "Y"
    else:
        return "N"


#reading the File
df = spark.read.format("csv").option("inferSchema",True) \
    .option("path", "C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\dataset.csv").load()


df1 = df.toDF("name","age","city")

#using column object expression

parseFunction = udf(age_check,StringType())

df2 = df1.withColumn("adult",parseFunction("age"))

df2.printSchema()
df2.show()

#using column string or sql expression

spark.udf.register("parseAge",age_check,StringType())
df3 = df1.withColumn("adult_2",expr("parseAge(age)"))

df3.printSchema()
df3.show()

listf = spark.catalog.listFunctions()


for x in listf:
    print(x)