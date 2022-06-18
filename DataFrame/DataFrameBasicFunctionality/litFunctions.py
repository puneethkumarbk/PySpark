#pySpark lit functions

from pyspark.sql.functions import lit, when, split, concat_ws
from pyspark.sql.functions import col

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

sparkConf = SparkConf()
sparkConf.set("spark.app.name", 'litFunctions')
sparkConf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

data = [("111",50000),("222",60000),("333",40000)]
columns= ["EmpId","Salary"]
df = spark.createDataFrame(data = data, schema = columns)

df.select(col("EmpId"),col("Salary"),lit("1").alias("lit_value")).show()

df.withColumn("lit_values", when((col("Salary") > 20000) & (col("Salary") <= 50000), lit("normal"))
              .otherwise("low")).show()

#split array to strings
data = [("James, A, Smith","2018","M",3000),
        ("Michael, Rose, Jones","2010","M",4000),
        ("Robert,K,Williams","2010","M",4000),
        ("Maria,Anne,Jones","2005","F",4000),
        ("Jen,Mary,Brown","2010","",-1)
        ]

columns=["name","dob_year","gender","salary"]
df=spark.createDataFrame(data,columns)
df.show()
df.printSchema()

df2 = df.select(split(col("name"), ",").alias("splitted_name"))
df2.printSchema()
df2.show()

df.createOrReplaceTempView("PERSON")
spark.sql("select SPLIT(name,',') as NameArray from PERSON").show()


#column array to string using concat_ws()

columns = ["name","languagesAtSchool","currentState"]
data = [("James,,Smith",["Java","Scala","C++"],"CA"), \
        ("Michael,Rose,",["Spark","Java","C++"],"NJ"), \
        ("Robert,,Williams",["CSharp","VB"],"NV")]

df = spark.createDataFrame(data=data,schema=columns)
df.printSchema()
df.show(truncate=False)

df2 = df.withColumn("new_column",concat_ws(",",col("languagesAtSchool")))
df2.printSchema()
df2.show()