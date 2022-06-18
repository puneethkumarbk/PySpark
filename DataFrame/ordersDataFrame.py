# DataFrame Reader and Writer Api examples
from pyspark import SparkConf
from pyspark.sql import SparkSession

sparkConf = SparkConf()
#creating SparkSession
sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "CoursePerPopularity")

spark = SparkSession.builder.config(conf = sparkConf).getOrCreate()

#setted Log Lelel to ERRoR
spark.sparkContext.setLogLevel("ERROR")



#reading the File
customer_orders = spark.read.format("csv").option("inferSchema",True)\
    .option("header",True)\
    .option("path", "C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\orders.csv").load()


customer_orders.printSchema()
customer_orders.show()


grouped_df = customer_orders.select("order_id", "order_customer_id").where("order_customer_id > 1000")\
    .groupBy("order_customer_id")\
    .count()

grouped_df.show()

print(grouped_df.rdd.getNumPartitions()) #200 dataframe by default has 200 partitoins
grouped_df = grouped_df.repartition(2)
print(grouped_df.rdd.getNumPartitions())



#repartition will not give us for partition pruning
"""grouped_df.write.format("csv").\
    mode("overwrite")\
    .option("path","C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\output2")\
    .save()"""

customer_orders.write.format("csv"). \
    mode("overwrite") \
    .partitionBy("order_status")\
    .option("maxRecordsperFile",200)\
    .option("path","C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\output2") \
    .save()