#Finding the popular Course based on the chapter views
from pyspark import SparkConf
from pyspark.sql import SparkSession

#creating SparkSession
sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "Acccumulator")

spark = SparkSession.builder.config(conf = sparkConf).getOrCreate()

#setted Log Lelel to ERRoR
spark.sparkContext.setLogLevel("ERROR")


#reading the File
views = spark.sparkContext.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\Assaignment\\views*.csv")
chapters = spark.sparkContext.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\Assaignment\\chapters.csv")

mapped_view = views.map(lambda x: (x.split(",")[1],x.split(",")[0]))
mapped_chapter = chapters.map(lambda x: (x.split(",")[0],x.split(",")[1]))

joined_rdd = mapped_view.join(mapped_chapter).map(lambda x: (int(x[1][1]),int(x[0])))
course_id_chapterCount = joined_rdd.reduceByKey(lambda x,y: x+y)


joined_rdd2 = mapped_view.distinct().join(mapped_chapter).map(lambda x: ((int(x[1][0]),int(x[1][1])),1))

view_count = joined_rdd2.map(lambda x:(x[0][1],x[1])).reduceByKey(lambda x,y: x+y)

join_view_count = view_count.join(course_id_chapterCount)
res = join_view_count.mapValues(lambda x: (x[0]/x[1])).collect()

for i in res:
    print(i)