#Check the Age and appen the String Y or N
from pyspark import SparkConf
from pyspark.sql import SparkSession

sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "filterByAge")

spark = SparkSession.builder.config(conf = sparkConf).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def parsed_data(line):
    words = line.split(",")
    if int(words[1]) > 18:
        return (words[0],words[1],words[2],"Y")
    else:
        return (words[0],words[1],words[2],"N")


rdd = spark.sparkContext.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\data.txt")


mappedData = rdd.map(parsed_data)

res = mappedData.collect()

for i in res:
    print(i)