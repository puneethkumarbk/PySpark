#sortby example in rdd
from pyspark import SparkContext

sc = SparkContext("local[*]","custermor")


#customer who spend the most
baserdd = sc.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\customerorders.csv")

rdd2 = baserdd.map(lambda x : (x.split(",")[0], float(x.split(",")[2]))).reduceByKey(lambda x,y : x+y)

res = rdd2.sortBy(lambda x: x[1]).collect()

for i in res:
    print(i)


