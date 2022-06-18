#Find the number of empty lines using Accumulator

from pyspark import SparkConf
from pyspark.sql import SparkSession

#creating SparkSession
sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "Acccumulator")

spark = SparkSession.builder.config(conf = sparkConf).getOrCreate()

#setted Log Lelel to ERRoR
spark.sparkContext.setLogLevel("ERROR")

def parser(line):
    #if len of the line is 0 then add the increment one to the accumulator
    if len(line) == 0:
        myaccum.add(1)


#reading the File
baseRdd = spark.sparkContext.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\sampleFile.txt")

#creating the accumulator
myaccum = spark.sparkContext.accumulator(0)

baseRdd.foreach(parser)
print(myaccum.value)



