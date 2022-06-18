# creating the schema explicitly using StructType in spark DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from pyspark import SparkConf
from pyspark.sql import SparkSession

#creating SparkSession
sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "ordersSchema")

spark = SparkSession.builder.config(conf = sparkConf).getOrCreate()

#setted Log Lelel to ERRoR
spark.sparkContext.setLogLevel("ERROR")

orders_shema = StructType([StructField("order_id",IntegerType(),False),
                           StructField("order_date", TimestampType(),False),
                           StructField("order_customer_id",IntegerType(),False),
                           StructField("order_status",StringType(),False)])


ddl_schema = "order_id Integer, order_date1 Timestamp,order_cust_id Integer,order_status String"


#reading the File
customer_orders = spark.read.format("csv").option("header",True).option("inferSchema",True)\
    .option("path", "C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\orders.csv").load()



customer_orders.printSchema()
customer_orders.show()
