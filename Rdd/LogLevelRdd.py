#Creating the Rdd with list of data using parallelize method
from pyspark import SparkConf
from pyspark.sql import SparkSession

sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "rddexamples3")

spark = SparkSession.builder.config(conf = sparkConf).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

if __name__ == "__main__":
    myList = ["ERROR: Thu Jun 04 10:37:51 BST 2015",
    "WARN: Sun Nov 06 10:37:51 GMT 2016",
    "WARN: Mon Aug 29 10:37:51 BST 2016",
    "ERROR: Thu Dec 10 10:37:51 GMT 2015",
    "ERROR: Fri Dec 26 10:37:51 GMT 2014",
    "ERROR: Thu Feb 02 10:37:51 GMT 2017",
    "WARN: Fri Oct 17 10:37:51 BST 2014",
    "ERROR: Wed Jul 01 10:37:51 BST 2015"]


    logs_rdd = spark.sparkContext.parallelize(myList)

    print("running from same file")
else:
    logs_rdd = spark.sparkContext.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\bigLogtxt-201014-183159\\bigLog.txt")

mapped_rdd = logs_rdd.map(lambda x: (x.split(":")[0],1)).reduceByKey(lambda x,y:x+y)

res = mapped_rdd.collect()

for i in res:
    print(i)


