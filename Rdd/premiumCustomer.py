#Find the top premium coustomers using rdd
from pyspark import SparkConf
from pyspark.sql import SparkSession
from sys import stdin
from pyspark import StorageLevel

#creating SparkSession
sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "CoursePerPopularity")

spark = SparkSession.builder.config(conf = sparkConf).getOrCreate()

#setted Log Lelel to ERRoR
spark.sparkContext.setLogLevel("ERROR")


#reading the File
customer_orders = spark.sparkContext.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\customerorders.csv")

prem_customer = customer_orders.map(lambda x: (x.split(",")[0],int(x.split(",")[1])))\
    .reduceByKey(lambda x,y:(x+y)).filter(lambda x: x[1] > 5000).persist(StorageLevel.MEMORY_ONLY)

double_amt = prem_customer.map(lambda x: (x[0],x[1]*2))


res = prem_customer.collect()

for i in res:
    print(i)

print(double_amt.count())

stdin.readline()