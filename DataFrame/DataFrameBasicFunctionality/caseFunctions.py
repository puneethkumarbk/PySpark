# pyspark DataFrame case functions Example
from pyspark.sql.functions import when, expr
from pyspark.sql.functions import col

from pyspark.conf import SparkConf

from pyspark.sql import SparkSession

sparkConf = SparkConf()
sparkConf.set("spark.app.name", 'caseFunctions')
sparkConf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

data = [("James","M",60000),("Michael","M",70000),
        ("Robert",None,400000),("Maria","F",500000),
        ("Jen","",None)]

columns = ["name","gender","salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.show()


df2 = df.withColumn("new_gender", when(df.gender == "M","Male")
                    .when(df.gender == "F","Female")
                    .when(df.gender.isNull() ,"")
                    .otherwise(df.gender))

df2.select(col("*"),when(df.gender == "M","male")
           .when(df.gender == "F","Female")
           .when(df.gender.isNull(), "rrr")
           .otherwise("none")).show()

df.select(col("*"), expr("case when gender == 'M' then 'Male'"+
                         "when gender == 'F' then 'Female'"+
                         "when gender is null then 'abcd'"+
                          "else 'None' end")).show()



df.withColumn("new_gender", when((df.gender == "M") | (df.gender == "F"),"MF")
                    .when((df.gender.isNull()) & (df.gender== ""),"lllll")
                    .otherwise(0)).show()