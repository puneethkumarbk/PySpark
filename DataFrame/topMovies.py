# Finding the Top movies using Spark DataFrame

from pyspark.sql.functions import desc

from pyspark.sql.functions import count, first, avg, broadcast

from pyspark import SparkConf
from pyspark.sql import SparkSession

#creating SparkSession
sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "FindingTopMovie")

spark = SparkSession.builder.config(conf = sparkConf).enableHiveSupport().getOrCreate()

#setted Log Level to ERROR
spark.sparkContext.setLogLevel("ERROR")

#using Rdd approch
rating_rdd = spark.sparkContext.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\ratings.dat")
movies_rdd = spark.sparkContext.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\movies.dat")

def parser(x):
    fields = x.split("::")
    return (fields[1],fields[2],fields[3])

ratings_df = rating_rdd.map(parser).toDF(schema= ["movie_id","movie_rating","no_of_views"])

movies_df = movies_rdd.map(lambda x: (x.split("::")[0], x.split("::")[1])).toDF(schema= ["movie_id","movie_name"])

movie_group= ratings_df.groupBy("movie_id").agg(count("movie_rating").alias("movie_count"),avg("movie_rating").alias("avg_movie_rating")) \
                                                            .orderBy("movie_count",ascending = False)

rating_filtered = movie_group.filter(movie_group.movie_count > 4)

#braoadcast join
joined_df = rating_filtered.join(broadcast(movies_df),rating_filtered.movie_id == movies_df.movie_id,"inner").drop(movies_df.movie_id)

top_movies = joined_df.drop("movie_id").sort(desc("movie_count"),desc("movie_count"))

top_movies.select("movie_name").show(False)