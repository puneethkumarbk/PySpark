#explode and lit Functions in PySpark Examples
from pyspark.sql.functions import explode, concat_ws, lit

from pyspark import SparkConf
from pyspark.sql.session import SparkSession

sparkConf = SparkConf()
sparkConf.set("spark.app.name",'explodeFunctions')
sparkConf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()


data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]
rdd=spark.sparkContext.parallelize(data)
rdd2 = rdd.flatMap(lambda x: x.split(" "))
for element in rdd2.collect():
    print(element)

arrayData = [
    ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
    ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
    ('Robert',['CSharp',''],{'hair':'red','eye':''}),
    ('Washington',None,None),
    ('Jefferson',['1','2'],{})]

df3 = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])
df3.show()

df3.select(df3.name,explode(df3.knownLanguages)).show()

df3.select(df3.name,explode(df3.properties)).show()

data = [('James','Smith','M',30),('Anna','Rose','F',41),
        ('Robert','Williams','M',62),
        ]
columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.show()

df.select(concat_ws(",",df.firstname,df.lastname).alias("fullname"),df.gender,lit(df.salary*2).alias("new_salary")).show()


# Another example
df2 = df.foreach(lambda x:print("Data ==>"+x["firstname"]+","+x["lastname"]+","+x["gender"]+","+str(x["salary"]*2)))

print(df2)