#window Aggregate example using spark DataFrame

from pyspark.sql.functions import avg, col, count, countDistinct

from pyspark.sql.functions import *

from pyspark import SparkConf
from pyspark.sql import SparkSession, Window

#creating SparkSession
sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "simpleAggregate")

spark = SparkSession.builder.config(conf = sparkConf).enableHiveSupport().getOrCreate()

#setted Log Lelel to ERRoR
spark.sparkContext.setLogLevel("ERROR")



#reading the File
customer_orders = spark.read.format("csv") \
    .option("inferSchema",True) \
    .option("path", "C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\windowdata.csv").load()


orders = customer_orders.toDF("Country","weekNum", "other", "Quantity","invoiceValue")

orders.show()

mywindow = Window.partitionBy("Country").orderBy("weekNum").rowsBetween(Window.unboundedPreceding, Window.currentRow)

mydf = orders.withColumn("RunningTotoal", sum("invoiceValue").over(mywindow))
mydf.show()