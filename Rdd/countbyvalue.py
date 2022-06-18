#Count by value example
from pyspark import SparkConf
from pyspark.sql import SparkSession

sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "rddexamples3")

spark = SparkSession.builder.config(conf = sparkConf).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

rdd = spark.sparkContext.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\sample.txt")

flattenrdd = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: x.upper())

res = flattenrdd.countByValue()
print(res)
for c in res:
    print(c)
"""
#sortbykey
flattenrdd2 = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x.upper(),1)).reduceByKey(lambda x,y: x+y)
res2 = flattenrdd2.map(lambda x: (x[1],x[0])).sortByKey(False).map(lambda x: (x[1],x[0])).collect()

for t in res2:
    print(t)

#using SoryBy
flattenrdd3 = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x.upper(),1)).reduceByKey(lambda x,y: x+y)
res3 = flattenrdd3.sortBy(lambda x: x[1],False).collect()

for i in res3:
    print(i)"""

