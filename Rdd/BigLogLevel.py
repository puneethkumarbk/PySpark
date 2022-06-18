#Groupby key and reduceby Examples

from pyspark import SparkConf
from pyspark.sql import SparkSession

#creating SparkSession
sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "GroupByExamples")

spark = SparkSession.builder.config(conf = sparkConf).getOrCreate()

#setted Log Lelel to ERRoR
spark.sparkContext.setLogLevel("ERROR")

#reduceByKeyCode
print("reduce by key example")


#reading the File
baseRdd = spark.sparkContext.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\bigLog.txt")

reducebyRes = baseRdd.map(lambda x: (x.split(":")[0],1)).reduceByKey(lambda x,y : x+y).collect()


for i in reducebyRes:
    print(i)


print("group by key example")


reducebyRes = baseRdd.map(lambda x: (x.split(",")[0],x.split(",")[1]))
groupedRdd = reducebyRes.groupByKey()
groupbyRes = groupedRdd.map(lambda x:(x[0],len(x[1]))).collect()


for i in groupbyRes:
    print(i)

