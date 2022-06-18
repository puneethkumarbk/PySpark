#Count by value Example in Rdd
from pyspark import SparkContext

sc = SparkContext("local[*]","movie-rating")


#customer who spend the most
print("using count by value")
moviedata = sc.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\moviedata.data")

rating = moviedata.map(lambda x : x.split("\t")[2])

res = rating.countByValue()

print(res)


print("using reducybyKey")
moviedata = sc.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\moviedata.data")

rating = moviedata.map(lambda x : (x.split("\t")[2],1))

res = rating.reduceByKey(lambda x,y : x + y).collect()

for i in res:
    print(i)