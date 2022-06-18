# PySpark Window Function example
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, rank, percent_rank, cume_dist, ntile, lead, lag
from pyspark.sql.window import Window

sparkConf = SparkConf()
sparkConf.set("spark.app.name", 'windowFunctions')
sparkConf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

simpleData = (("James", "Sales", 3000), \
              ("Michael", "Sales", 4600), \
              ("Robert", "Sales", 4100), \
              ("Maria", "Finance", 3000), \
              ("James", "Sales", 3000), \
              ("Scott", "Finance", 3300), \
              ("Jen", "Finance", 3900), \
              ("Jeff", "Marketing", 3000), \
              ("Kumar", "Marketing", 2000), \
              ("Saif", "Sales", 4100) \
              )

columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.show(truncate=False)


mywindow= Window.partitionBy("department").orderBy("salary")
"""
df.withColumn("row_number",row_number().over(mywindow)).show()

df.withColumn("rank",rank().over(mywindow)).show()

df.withColumn("p_rank",percent_rank().over(mywindow)).show()

df.withColumn("c_dist",cume_dist().over(mywindow)).show()


df.withColumn("ntile",ntile(4).over(mywindow)).show()

"""

windowSpecAgg  = Window.partitionBy("department")
from pyspark.sql.functions import col,avg,sum,min,max,row_number
df.withColumn("row",row_number().over(mywindow)) \
    .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
    .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
    .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
    .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
    .where(col("row")==1).select("department","avg","sum","min","max") \
    .show()

df.withColumn("lead_f",lead("salary",2).over(mywindow)).show()
df.withColumn("lag",lag("salary",2).over(mywindow)).show()
