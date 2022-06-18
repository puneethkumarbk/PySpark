# Find the top rating movie using rdd
from pyspark import SparkConf
from pyspark.sql import SparkSession


#movie should atleast 1000 people have rated
#rating should be greater than 4.5


#creating SparkSession
sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "topRatedMovie")

spark = SparkSession.builder.config(conf = sparkConf).getOrCreate()


#reading the File
movies = spark.sparkContext.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\movies.dat")
ratings = spark.sparkContext.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\ratings.dat")

mapped_ratings =mapped_ratings = ratings.map(lambda x: (x.split("::")[1],x.split("::")[2])).mapValues(lambda x: (int(x[0]),1.0))

rating_res = mapped_ratings.reduceByKey(lambda x,y: (x[0] + y[0],x[1]+y[1])).filter(lambda x:x[1][1] >= 3)

filter_rating_res = rating_res.mapValues(lambda x: (x[0]/x[1])).filter(lambda x: x[1] > 2.5)


movies_data = movies.map(lambda x: (x.split("::")[0],x.split("::")[1]))

final_top_movie = filter_rating_res.join(movies_data).map(lambda x: x[1][1]).collect()

for i in final_top_movie:
    print(i)

#stop the spark Session
spark.stop()