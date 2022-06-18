# pySpark DataFrame date and time Functions

from pyspark.sql.functions import to_timestamp, lit, to_date, current_timestamp, current_date, date_format

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

sparkConf = SparkConf()
sparkConf.set("spark.app.name", 'dateandtime')
sparkConf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()


df=spark.createDataFrame(
    data = [ ("1","2019-06-24 12:01:19.000")],
    schema=["id","input_timestamp"])
df.printSchema()

#Timestamp String to DateType
df.withColumn("timestamp",to_timestamp("input_timestamp")) \
    .show(truncate=False)

# Using Cast to convert TimestampType to DateType
df.withColumn('timestamp_string', to_timestamp('input_timestamp').cast('string')).show(truncate=False)

df.select(to_timestamp(lit('06-24-2019 12:01:19.000'),'MM-dd-yyyy HH:mm:ss.SSSS')).show()


#SQL string to TimestampType
spark.sql("select to_timestamp('2019-06-24 12:01:19.000') as timestamp")
#SQL CAST timestamp string to TimestampType
spark.sql("select timestamp('2019-06-24 12:01:19.000') as timestamp")
#SQL Custom string to TimestampType
spark.sql("select to_timestamp('06-24-2019 12:01:19.000','MM-dd-yyyy HH:mm:ss.SSSS') as timestamp")


df=spark.createDataFrame(
    data = [ ("1","2019-06-24 12:01:19.000")],
    schema=["id","input_timestamp"])
df.printSchema()

df.withColumn("date_type",to_date(current_timestamp())) \
    .show(truncate=False)

df.select(to_date(lit('06-24-2019 12:01:19.000'))) \
    .show()

df=spark.createDataFrame([["1"]],["id"])
df.select(current_date().alias("current_date"), \
          date_format(current_timestamp(),"yyyy MM dd").alias("yyyy MM dd"), \
          date_format(current_timestamp(),"MM/dd/yyyy hh:mm").alias("MM/dd/yyyy"), \
          date_format(current_timestamp(),"yyyy MMM dd").alias("yyyy MMMM dd"), \
          date_format(current_timestamp(),"yyyy MMMM dd E").alias("yyyy MMMM dd E") \
          ,df.id).show()
