from pyspark import SparkContext
sc = SparkContext("local[*]", "DE - ETL - 102")

data = [
    {"id": 1, "name": "Hieu"},
    {"id": 2, "name": "Dung"},
    {"id": 3, "name": "Dat"}
]
rdd = sc.parallelize(data)

print(rdd.getNumPartitions())
