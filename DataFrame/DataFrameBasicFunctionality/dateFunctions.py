# Spark Date Functions

from pyspark.sql.functions import *

from pyspark import SparkConf
from pyspark.sql import SparkSession



sparkConf = SparkConf()
sparkConf.set("spark.app.name", 'dateFunctions')
sparkConf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

data=[["1","2020-02-01"],["2","2019-03-01"],["3","2021-03-01"]]
df=spark.createDataFrame(data,["id","input"])
df.show()


df.select(current_date().alias("current_date")).show()
df.select(date_format(col("input"),"mm-yyyy").alias("current_frmt")).show(2)

#date_format()
df.select(col("input"),date_format(col("input"), "MM-dd-yyyy").alias("date_format")).show()
col("input"),
#to date
df.select(col("input"), to_date(col("input"),"yyyy-mm-dd")).show()
df.select(col("input"), datediff(current_date(), col("input")).alias("date_diff")).show()


df.select(col("input"),
          add_months(col("input"),3).alias("add_months"),
          add_months(col("input"),-3).alias("sub_months"),
          date_add(col("input"),4).alias("date_add"),
          date_sub(col("input"),4).alias("date_sub")
          ).show()


df.select(col("input"),
          year(col("input")).alias("year"),
          month(col("input")).alias("month"),
          next_day(col("input"),"Sunday").alias("next_day"),
          weekofyear(col("input")).alias("weekofyear")
          ).show()


df.select(col("input"),
          dayofweek(col("input")).alias("dayofweek"),
          dayofmonth(col("input")).alias("dayofmonth"),
          dayofyear(col("input")).alias("dayofyear"),
          ).show()

#current_timestamp()
df.select(current_timestamp().alias("current_timestamp")).show(1,truncate=False)

df.select(col("input"),to_timestamp(col("input"), "MM-dd-yyyy HH mm ss SSS").alias("to_timestamp")).show(truncate=False)



df.select(col("input"),
           hour(col("input")).alias("hour"),
           minute(col("input")).alias("minute"),
           second(col("input")).alias("second")
           ).show(truncate=False)