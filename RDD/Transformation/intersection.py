from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE - ETL - 102", ).setMaster("local[*]").set("spark.execute.memory", "4g")

sc = SparkContext(conf=conf)

rdd1 = sc.parallelize([1,2,3,4,5,6,7,8])
rdd2 = sc.parallelize([1,9,10,11,12,13,14])

rdd3 = rdd1.intersection(rdd2)
print(rdd3.collect())

