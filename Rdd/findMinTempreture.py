#Find the Minumum Tempreture in the given data
from pyspark import SparkConf
from pyspark.sql import SparkSession

sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "findMinTempreture")

spark = SparkSession.builder.config(conf = sparkConf).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


def parsed_data(line):
    words = line.split(",")
    return (words[0],words[2],words[3])



rdd = spark.sparkContext.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\tempdata.csv")


mappedData = rdd.map(parsed_data).filter(lambda x: x[1] == "TMIN").map(lambda x: (x[0],float(x[2])))

res = mappedData.reduceByKey(lambda x,y: min(x,y)).collect()

for i in res:
    print(i)
