# Pyspark Array Examples
from pyspark.sql.functions import explode, split, array, array_contains

from pyspark import SparkConf
from pyspark.sql.session import SparkSession

sparkConf = SparkConf()
sparkConf.set("spark.app.name", 'ArrayTypeExample')
sparkConf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

data = [
    ("James,,Smith", ["Java", "Scala", "C++"], ["Spark", "Java"], "OH", "CA"),
    ("Michael,Rose,", ["Spark", "Java", "C++"], ["Spark", "Java"], "NY", "NJ"),
    ("Robert,,Williams", ["CSharp", "VB"], ["Spark", "Python"], "UT", "NV")
]

from pyspark.sql.types import StringType, ArrayType, StructType, StructField

schema = StructType([
    StructField("name", StringType(), True),
    StructField("languagesAtSchool", ArrayType(StringType()), True),
    StructField("languagesAtWork", ArrayType(StringType()), True),
    StructField("currentState", StringType(), True),
    StructField("previousState", StringType(), True)
])

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show()

df.select(df.name, explode(df.languagesAtSchool)).show()

df.select(split(df.name, ",").alias("names_as_array")).show()
df.select(split(df.name, ",").alias("names_as_array")).printSchema()

df.select(df.name, array(df.previousState, df.currentState)).show()

df.select(df.name, array_contains(df.languagesAtWork, "Java"))
