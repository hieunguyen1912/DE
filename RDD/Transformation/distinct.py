from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE - ETL - 102", ).setMaster("local[*]").set("spark.execute.memory", "4g")

sc = SparkContext(conf=conf)

data = sc.parallelize(["one", 1, 2, "two", "three", 3, "one", "two", 1, 2])

duplicateRdd = data.distinct()

print(duplicateRdd.collect())





