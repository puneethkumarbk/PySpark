# pySpark StringFunctions
from pyspark.sql.functions import substring, regexp_replace, when, translate

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

sparkConf = SparkConf()
sparkConf.set("spark.app.name", 'stringFunctions')
sparkConf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

data = [(1,"20200828"),(2,"20180525")]
columns=["id","date"]
df=spark.createDataFrame(data,columns)
df.show()

df.withColumn("year",substring('date',1,4)).withColumn("month",substring('date',5,2))\
    .withColumn("day",substring('date',7,2)).show()

address = [(1,"14851 Jeffrey Rd","DE"),
           (2,"43421 Margarita St","NY"),
           (3,"13111 Siemon Ave","CA")]
df =spark.createDataFrame(address,["id","address","state"])
df.show()



df.withColumn('address_2',regexp_replace("address","Rd","Road")).show()


df.withColumn('address_3',
              when(df.address.endswith("df"),regexp_replace(df.address,"Rd","Road"))
              .when(df.address.endswith("St"),regexp_replace(df.address,"St","State"))
              .when(df.address.endswith("Ave"),regexp_replace(df.address,"Ave","Avenue"))
              .otherwise(df.address)).show()


stateDic={'CA':'California','NY':'New York','DE':'Delaware'}

rdd2 = df.rdd.map(lambda x:(x.id,x.address,stateDic[x.state]))
rdd2.foreach(print)

df.withColumn('address',translate(df.address,'123','ABc')).show()


#Overlay
from pyspark.sql.functions import overlay
df = spark.createDataFrame([("ABCDE_XYZ", "FGH")], ("col1", "col2"))
df.select(overlay(df.col1,df.col2,4,3)).show()