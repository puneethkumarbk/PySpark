# spark sql examples
from pyspark import SparkConf
from pyspark.sql import SparkSession

#creating SparkSession
sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "CoursePerPopularity")

spark = SparkSession.builder.config(conf = sparkConf).getOrCreate()

#setted Log Lelel to ERRoR
spark.sparkContext.setLogLevel("ERROR")



#reading the File
customer_orders = spark.read.format("csv").option("inferSchema",True) \
    .option("header",True) \
    .option("path", "C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\orders.csv").load()

customer_orders.createOrReplaceTempView("orders")

spark.sql("select  order_status,count(order_status) from orders group by order_status").show()