#Grouping Aggregate Examples using spark Dataframe

from pyspark.sql.functions import avg, col, count, countDistinct

from pyspark.sql.functions import *

from pyspark import SparkConf
from pyspark.sql import SparkSession

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
    .option("header",True) \
    .option("path", "C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\order_data.csv").load()



#column object

customer_orders.groupBy("Country","InvoiceNo").agg(sum(expr("Quantity * UnitPrice"))).alias("invoiceValue").show()


#columns String

customer_orders.groupBy("Country","InvoiceNo").agg(expr("sum(Quantity * UnitPrice) as invoiceValue")).show()

#sparkSql

customer_orders.createOrReplaceTempView("groupingorders")

spark.sql("select Country,InvoiceNo, sum(Quantity * UnitPrice) as invoiceValue2 from groupingorders group by Country,InvoiceNo").show()