# Handling Unstructured data using RegExp in Spark DataFrame
from pyspark.sql.functions import regexp_extract

from pyspark import SparkConf
from pyspark.sql import SparkSession
#creating SparkSession
sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "CoursePerPopularity")

spark = SparkSession.builder.config(conf = sparkConf).enableHiveSupport().getOrCreate()

#setted Log Lelel to ERRoR
spark.sparkContext.setLogLevel("ERROR")

my_regexp = r"^(\S+) (\S+)\t(\S+)\,(\S+)"

lines_df = spark.read.text("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\neworders.csv")

lines_df.printSchema()
lines_df.show(truncate=False)

final_df = lines_df.select(regexp_extract('value',my_regexp,1).alias("order_id")
                           ,regexp_extract('value',my_regexp,2).alias("order_date")\
                            ,regexp_extract('value',my_regexp,3).alias("cust_id")\
                           ,regexp_extract('value',my_regexp,4).alias("status"))


final_df.printSchema()
final_df.show()


