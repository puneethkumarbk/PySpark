# spark Aggregate Functions

from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import approx_count_distinct, avg, collect_list, collect_set, countDistinct, first, last, \
    stddev, stddev_samp, stddev_pop, variance, var_samp, var_pop
from pyspark.sql.session import SparkSession

sparkConf = SparkConf()
sparkConf.set("spark.app.name", 'AggregateExample')
sparkConf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()


simpleData = [("James", "Sales", 3000),
              ("Michael", "Sales", 4600),
              ("Robert", "Sales", 4100),
              ("Maria", "Finance", 3000),
              ("James", "Sales", 3000),
              ("Scott", "Finance", 3300),
              ("Jen", "Finance", 3900),
              ("Jeff", "Marketing", 3000),
              ("Kumar", "Marketing", 2000),
              ("Saif", "Sales", 4100)
              ]
schema = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)

df.select(approx_count_distinct("salary")).show()

#approx_count_distinct()
print("approximate distinct count:"+ str(df.select(approx_count_distinct("salary")).collect()[0][0]))

print("avg"+ str(df.select(avg("salary")).collect()[0][0]))



print(df.select(collect_list("salary")).show(truncate=False))

print(df.select(collect_set("salary")).show(truncate= False))


df2 = df.select(countDistinct("department", "salary"))
df2.show(truncate=False)
print("Distinct Count of Department & Salary: "+str(df2.collect()[0][0]))



#first
df.select(first("salary")).show(truncate=False)

#last
df.select(last("salary")).show(truncate=False)

df.select(stddev("salary"),stddev_samp("salary"),stddev_pop("salary")).show()
df.select(variance("salary"),var_samp("salary"),var_pop("salary")).show(truncate=False)




