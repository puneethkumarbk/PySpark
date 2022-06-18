#calculate the employee count based on dept wise
from pyspark.sql.functions import count, first

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
employee = spark.read.format("json") \
    .option("path", "C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\employee.json").load()

#reading the File
dept = spark.read.format("json") \
    .option("path", "C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\dept.json").load()


employee.show()
dept.show()

joineddf = employee.join(dept, employee.deptid == dept.deptid, "inner").drop(dept.deptid)

joineddf.show()

finaldf = joineddf.groupBy("deptid").agg(count("empname").alias("emp_count"),first("deptName").alias("deptName"))

finaldf.show()