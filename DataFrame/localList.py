#Convert the local list to spark Dataframe example

from pyspark.sql.functions import unix_timestamp, monotonically_increasing_id, col, expr

from pyspark import SparkConf
from pyspark.sql import SparkSession

#creating SparkSession
sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "sparkLocalListCreation")

spark = SparkSession.builder.config(conf = sparkConf).enableHiveSupport().getOrCreate()

#setted Log Lelel to ERRoR
spark.sparkContext.setLogLevel("ERROR")

myList = [(1,"2013-07-25",11599,"CLOSED"),(2,"2014-07-25",256,"PENDING_PAYMENT"),
          (3,"2013-07-25",11599,"COMPLETE"),(4,"2019-07-25",8827,"CLOSED")]



ordersDf = spark.createDataFrame(myList).toDF("order_id","order_date","custemor_id","order_Status")

ordersDf.show()

#columns string
new_ordersDf = ordersDf.withColumn("date1",expr("unix_timestamp(order_date)"))\
    .withColumn("increasing_id",monotonically_increasing_id())\
    .dropDuplicates(["custemor_id","order_date"])\
    .sort("order_date")

new_ordersDf.show()


#columns object
new_ordersDf2 = ordersDf.withColumn("date1",unix_timestamp(col("order_date"))) \
    .withColumn("increasing_id",monotonically_increasing_id()) \
    .dropDuplicates(["custemor_id","order_date"]) \
    .sort(col("order_date"))

new_ordersDf2.show()
