from pyspark import SparkContext

sc = SparkContext("local[*]","dummy")

#reading the File
baseRdd = sc.textFile("C:\\Users\\Hp\\IdeaProjects\\PySpark\\Resources\\sampleFile.txt")



print(sc.defaultParallelism)
print(sc.defaultMinPartitions)
print(baseRdd.getNumPartitions())

