#Find the Number of Friends Based on Age group using Rdd
from pyspark import SparkContext

sc = SparkContext("local[*]","movie-rating")


def split_func(lines):
    lst = lines.split("::")
    return (lst[2],float(lst[3]))



#calculate the friends by age

friendsdata = sc.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\friendsdata.csv")

"""
0::Will::33::385
1::Jean-Luc::26::2

map
=======
33,(385,1)
33,(385,1)
=======

reducebykey
============
33,(385,2)

map
=====
385/2
(33,385/2)
"""

split_data = friendsdata.map(split_func)

#using map  values
#avgdata = split_data.mapValues(lambda x : (x,1)).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))

avgdata = split_data.map(lambda x : (x[0],(x[1],1))).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))


resdata = avgdata.map(lambda x: (x[0],x[1][0]//x[1][1])).sortBy(lambda x: x[1],False).collect()


for i in resdata:
    print(i)