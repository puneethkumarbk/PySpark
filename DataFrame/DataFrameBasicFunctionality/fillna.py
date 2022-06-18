from pyspark import SparkConf
from pyspark.sql.session import SparkSession
sparkConf = SparkConf()
sparkConf.set("spark.app.name",'flatmapExample')
sparkConf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()


df = spark.read.format("csv").option("header", True)\
.option("inferschema", True)\
.option("path", "C:\\Users\\Hp\\Desktop\\shared\\pySpark\\inputfiles\\sample2.csv")\
.load()
df2 = df.fillna(value = "fill_it",)
df2.show()

df3 = df.fillna({"city":"unknown","type":""})
df3.show()