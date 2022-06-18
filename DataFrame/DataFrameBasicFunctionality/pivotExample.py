# Spark Pivot Examples

from pyspark import SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import expr
sparkConf = SparkConf()
sparkConf.set("spark.app.name",'pivot')
sparkConf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"), \
        ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"), \
        ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"), \
        ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

columns= ["Product","Amount","Country"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)

df.groupby("Product").pivot("Country").sum("Amount").show()


countries = ["USA","China","Canada","Mexico"]
pivotDF = df.groupBy("Product").pivot("Country", countries).sum("Amount")
pivotDF.show(truncate=False)
pivotDF.printSchema()
df.groupby("Product","Country").sum("Amount").groupby("Product").pivot("Country").sum("sum(Amount)").show()
df.groupby("Product","Country").sum("Amount").groupby("Product").pivot("Country").sum("sum(Amount)").printSchema()



unpivotExpr = "stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"
unPivotDF = pivotDF.select("Product", expr(unpivotExpr)).where("Total is not null")
unPivotDF.show(truncate=False)
unPivotDF.show()
