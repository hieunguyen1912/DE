from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE - ETL - 102", ).setMaster("local[*]").set("spark.execute.memory", "4g")

sc = SparkContext(conf=conf)

fileRdd = sc.textFile("../Data/sample1.txt")

print(fileRdd.getNumPartitions())

#print(fileRdd.collect())