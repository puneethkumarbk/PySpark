#pySpark Array Examples
from pyspark.sql.functions import explode, flatten

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

sparkConf = SparkConf()
sparkConf.set("spark.app.name", 'arrayFunctions')
sparkConf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()
arrayArrayData = [
    ("James",[["Java","Scala","C++"],["Spark","Java"]]),
    ("Michael",[["Spark","Java","C++"],["Spark","Java"]]),
    ("Robert",[["CSharp","VB"],["Spark","Python"]])
]

df = spark.createDataFrame(data=arrayArrayData, schema = ['name','subjects'])
df.printSchema()
df.show(truncate=False)


df.select(df.name,explode(df.subjects)).show(truncate=False)

df.select(df.name,flatten(df.subjects)).show(trucate= False)