#find the popular Courses

from pyspark import SparkConf
from pyspark.sql import SparkSession

#creating SparkSession
sparkConf = SparkConf()

sparkConf.set("spark.app.master","local[*]")
sparkConf.set("spark.name", "CoursePerPopularity")

spark = SparkSession.builder.config(conf = sparkConf).getOrCreate()

#setted Log Lelel to ERRoR
spark.sparkContext.setLogLevel("ERROR")

def score_cal(score):
    if score > 0.09:
        return 10
    elif score < 0.09 and score > 0.05:
        return 4
    elif score < 0.05 and score > 0.025:
        return 4
    else:
        return 0







#reading the File
views = spark.sparkContext.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\Assaignment\\views*.csv")
chapters = spark.sparkContext.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\Assaignment\\chapters.csv")
titels = spark.sparkContext.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\Assaignment\\titles.csv")


mapped_view = views.map(lambda x: (x.split(",")[1],x.split(",")[0])).distinct()
mapped_chapter = chapters.map(lambda x: (x.split(",")[0],x.split(",")[1]))

chapterCount= mapped_view.join(mapped_chapter).map(lambda x:(x[1][1],int(x[0]))).reduceByKey(lambda x,y:x+y)



viewCount = mapped_view.join(mapped_chapter).map(lambda x: ((x[1][1],x[1][0]),1)).map(lambda x: (x[0][0],x[1])).reduceByKey(lambda x,y: x+y)

totalCount = viewCount.join(chapterCount).map(lambda x: (x[0],x[1][0]/x[1][1]))

score_count = totalCount.mapValues(score_cal).map(lambda x: (int(x[0]),x[1]))

mapped_titels = titels.map(lambda x: (int(x.split(",")[0]),x.split(",")[1]))

final_res = score_count.join(mapped_titels).map(lambda x: (x[0],x[1][1])).sortBy(lambda x:x[1]).collect()

for i in final_res:
    print(i)
"""
print("-----------------------------------")

aad = score_count.join(titels) #map(lambda x: (x[0],x[1][1]))



final_res = aad.collect()

for i in final_res:
    print(i)


print(" ===================================================================")
r = titels.collect()
for i in r:
    print(i) """