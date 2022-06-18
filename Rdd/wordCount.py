#Word Count Program using RDD
from pyspark import SparkConf
from pyspark.sql import SparkSession
from sys import stdin

sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "rddexamples")

spark = SparkSession.builder.config(conf = sparkConf).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

rdd = spark.sparkContext.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\sample.txt")

words = rdd.flatMap(lambda x: x.split(" ")).map(lambda x : (x,1))
wordscount = words.reduceByKey(lambda x,y: (x+y))
wd = wordscount.collect()
for w in wd:
    print(w)


stdin.readline()

"""rdd = spark.sparkContext.textFile("C:\\Users\\Hp\\Desktop\\shared\\pySpark\\inputfiles\\text_1.txt")
rdd2 = rdd.flatMap(lambda x: x.split(" "))
rdd3 = rdd2.map(lambda x: (x,1))
rdd4 = rdd3.reduceByKey(lambda x,y: (x+y))
rdd5 = rdd4.map(lambda x: (x[0], x[1])).sortBy(lambda x: x[1],False).filter(lambda x: "an" in x[0])
print(rdd5.collect())"""