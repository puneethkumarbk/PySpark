# PySpark json Functionality Examples
from pyspark.sql.types import MapType, StringType

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, json_tuple, get_json_object, schema_of_json, lit

sparkConf = SparkConf()
sparkConf.set("spark.app.name", 'jsonFunctions')
sparkConf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

jsonString="""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
df=spark.createDataFrame([(1, jsonString)],["id","value"])
df.show(truncate=False)
df.printSchema()


df2=df.withColumn("from_json",from_json(df.value,MapType(StringType(),StringType())))
df2.printSchema()
df2.show(truncate=False)


#df2.withColumn("value",to_json(col("value"))).show(truncate=False)

df.select(col("id"),json_tuple(col("value"),"Zipcode","ZipCodeType","City")).show(truncate=False)

df.select(col("id"),get_json_object(col("value"),"$.ZipCodeType").alias("ZipCodeType")) \
    .show(truncate=False)


schemaStr=spark.range(1) \
    .select(schema_of_json(lit("""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""))) \
    .collect()[0][0]
print(schemaStr)
