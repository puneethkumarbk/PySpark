# Handling partition skew/ Data skew problem using Salting Approach
import random
from pyspark import SparkConf
from pyspark.sql import SparkSession

#creating SparkSession
sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "simpleAggregate")

spark = SparkSession.builder.config(conf = sparkConf).enableHiveSupport().getOrCreate()

#setted Log Lelel to ERRoR
spark.sparkContext.setLogLevel("ERROR")

#normal approch
"""
biglogrdd = spark.sparkContext.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\biglog.txt")

mappedRdd = biglogrdd.map(lambda x: (x.split(",")[0],x.split(",")[1]))

groupedRdd = mappedRdd.groupByKey()

aggRes = groupedRdd.map(lambda x: (x[0], len(x[1])))

res = aggRes.collect()

for i in res:
    print(i)

"""

#using Salting

def randomGenerator(num):
    return random.randint(1,60)

def comparekeys(x):
    if (x[0][0:4] == "WARN"):
        return ("WARN",x[1])
    else:
        return ("ERROR",x[1])

biglogrdd = spark.sparkContext.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\biglog.txt")

mappedRdd = biglogrdd.map(lambda x: (x.split(",")[0]+str(randomGenerator(x[0])),x.split(",")[1]))

groupedRdd = mappedRdd.groupByKey()

rdd4 = groupedRdd.map(lambda x: (x[0], len(x[1])))

rdd5 = rdd4.cache()

rdd6 = rdd5.map(lambda x: comparekeys(x))

rdd7 = mappedRdd.reduceByKey(lambda x,y: x+y)

res = rdd7.collect()

for i in res:
    print(i)
