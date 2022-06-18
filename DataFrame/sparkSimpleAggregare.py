#Spark Simple Aggregate Examples uisng  DataFrame

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
customer_orders = spark.read.format("csv")\
    .option("inferSchema",True) \
    .option("header",True) \
    .option("path", "C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\order_data.csv").load()

#column object
customer_orders.select(count("*").alias("rowCount"),\
    sum(col("Quantity")).alias("total quantity"),\
    avg(col("UnitPrice")).alias("unit price"),\
    countDistinct(col("InvoiceNo")).alias("distict_count")).show()

#column String
customer_orders.selectExpr("count(*) as rowCount", \
                       "sum(Quantity) as total_quantity", \
                       "avg(UnitPrice) as unit_price", \
                       "count(Distinct(InvoiceNo)) as distict_count").show()


#Sql Style
customer_orders.createOrReplaceTempView("orders")

spark.sql("select count(*) as row_count,sum(Quantity) as totol_quantity, avg(UnitPrice) as avg_price, count(distinct(InvoiceNo)) as d_count from orders").show()




