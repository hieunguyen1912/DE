from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE - ETL - 102", ).setMaster("local[*]").set("spark.execute.memory", "4g")

sc = SparkContext(conf=conf)

rdd1 = sc.parallelize([
    {"id": 1, "name": "Hieu"},
    {"id": 2, "name": "Dung"},
    {"id": 3, "name": "Dat"}
])

rdd2 = sc.parallelize([1,2,3,4,5,6,7])
rdd3 = rdd1.union(rdd2)
print(rdd3.collect())