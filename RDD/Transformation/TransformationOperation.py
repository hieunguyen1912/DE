from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE - ETL - 102", ).setMaster("local[*]").set("spark.execute.memory", "4g")

sc = SparkContext(conf=conf)

numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
# [1, 4, 9, 16, 25, 36, 49, 64, 81, ....]

rdd = sc.parallelize(numbers)

squareRdd = rdd.map(lambda x: x * x)
filterRdd = rdd.filter(lambda x: x > 3)
flatMapRdd = rdd.flatMap(lambda x: [(x,x*2)])

print(flatMapRdd.collect())
