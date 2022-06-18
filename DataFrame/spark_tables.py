#creating the spark table with enabled hive support to store the metadata in Hive.
from pyspark import SparkConf
from pyspark.sql import SparkSession

#creating SparkSession
sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "CoursePerPopularity")

spark = SparkSession.builder.config(conf = sparkConf).enableHiveSupport().getOrCreate()

#setted Log Lelel to ERRoR
spark.sparkContext.setLogLevel("ERROR")



#reading the File
customer_orders = spark.read.format("csv").option("inferSchema",True) \
    .option("header",True) \
    .option("path", "C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\orders.csv").load()


spark.sql("create database if not exists test")
customer_orders.write.format("csv")\
    .mode("overwrite")\
    .bucketBy(3,"order_customer_id")\
    .sortBy("order_customer_id")\
    .saveAsTable("test.orders_table3")