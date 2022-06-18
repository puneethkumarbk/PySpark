#BroadCast join Example with Campaigndata excluding the bingo words from campaign data

from pyspark import SparkConf
from pyspark.sql import SparkSession


sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "braodcastjoinexample")

spark = SparkSession.builder.config(conf = sparkConf).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


def parsed_data(line):
    words = line.split(",")
    return (float(words[10]),words[0])

def boaring_words():
    boaringWords = set(i.strip() for i in open("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\bingoWords.txt"))
    return boaringWords

baseRdd = spark.sparkContext.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\bigdatacampaigndata.csv")

brodcastset = spark.sparkContext.broadcast(boaring_words())

mapped_data = baseRdd.map(parsed_data).flatMapValues(lambda x: x.split(" ")).map(lambda x: (x[1].lower(),x[0]))

brodcast_filter_data = mapped_data.filter(lambda x: x[0] not in brodcastset.value)

res = brodcast_filter_data.reduceByKey(lambda x,y: (x+y)).sortBy(lambda x:x[1],False).collect()

for i in res:
    print(i)