#PySpark sample Functions examples

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

from pyspark.sql import SparkSession
from pyspark import SparkConf
sparkConf = SparkConf()
sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "sampleExamples")

spark = SparkSession.builder.config(conf = sparkConf).getOrCreate()

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

rdd = spark.sparkContext.parallelize(data)
dfFromRdd1 = rdd.toDF()
dfFromRdd1.printSchema()

df2 = spark.createDataFrame(data = data,schema= columns)
df2.printSchema()

data2 = [("James","","Smith","36636","M",3000),
         ("Michael","Rose","","40288","M",4000),
         ("Robert","","Williams","42114","M",4000),
         ("Maria","Anne","Jones","39192","F",4000),
         ("Jen","Mary","Brown","","F",-1)
         ]

schema = StructType([StructField("first_name", StringType(),True), \
                     StructField("middlename",StringType(),True), \
                     StructField("lastname",StringType(),True), \
                     StructField("id", StringType(), True), \
                     StructField("gender", StringType(), True), \
                     StructField("salary", IntegerType(), True) \
                     ])

schema2 = "first_name string, middle_name string, last_name string,id string,gender string, salary long"

df3 = spark.createDataFrame(data = data2,schema= schema2)
df3.printSchema()

df3.show(200)
print("____________________________________________________________________________________")

df2 = spark.read.format("csv") \
    .option("header",True) \
    .option("inferSchema",True) \
    .option("path","C:\\Users\\Hp\\Desktop\\shared\\pySpark\\inputfiles\\df2.csv") \
    .load()

df2.show()

df2.write.format("parquet").mode("append").option("path","C:\\Users\\Hp\\Desktop\\shared\\pySpark\\inputfiles\\df3.csv").save()

