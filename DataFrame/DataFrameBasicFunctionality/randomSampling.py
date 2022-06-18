#PySpark RandomSampling Examples

from pyspark import SparkConf
from pyspark.sql.session import SparkSession

sparkConf = SparkConf()
sparkConf.set("spark.app.name",'samplingExample')
sparkConf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()


df=spark.range(100)
print(df.sample(0.06).collect())
print(df.sample(0.06).collect())


print("sssssssssssssssssssssssssssssssssssseed")


print(df.sample(0.1,123).collect())
print(df.sample(0.1,123).collect())
print(df.sample(0.1,456).collect())
print(df.sample(0.1,456).collect())

print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$withreplacement")
print(df.sample(withReplacement=True, fraction = 0.1,seed = 456).collect())
print(df.sample(withReplacement=False, fraction = 0.1,seed = 456).collect())