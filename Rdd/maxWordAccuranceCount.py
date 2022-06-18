#Find The Maximum Accurance of Word Using Rdd
from pyspark import SparkConf
from pyspark.sql import SparkSession

sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "rdd_examples")

spark = SparkSession.builder.config(conf = sparkConf).getOrCreate()

rdd = spark.sparkContext.textFile("C:\\Users\\Hp\\Desktop\\shared\\pySpark\\inputfiles\\text_1.txt")
rdd2 = rdd.flatMap(lambda x: x.split(" "))
rdd3 = rdd2.map(lambda x: (x,1))
rdd4 = rdd3.reduceByKey(lambda x,y: (x+y))
rdd5 = rdd4.map(lambda x: (x[0], x[1])).sortBy(lambda x: x[1],False).filter(lambda x: "an" in x[0])
print(rdd5.collect())

print(rdd5.count())
print("first value of rdd"+str(rdd5.first()))
print("max value of rdd"+str(rdd5.max()))

print("----------------------------------------")
print(rdd5.take(2))
