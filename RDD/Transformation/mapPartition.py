import time
from pyspark import SparkContext, SparkConf
from random import Random

conf = SparkConf().setAppName("DE - ETL - 102", ).setMaster("local[*]").set("spark.execute.memory", "4g")

sc = SparkContext(conf=conf)

data = ["ToanNguyen-debt", "Hieu-debt", "Duy-debt", "DucAnh-debt"]
rdd = sc.parallelize(data, 2)

# def partition(iterator):
#     rand = Random(int(time.time() * 1000) + Random().randint(0, 1000))
#
#     return [f"{item}: {rand.randint(0, 1000)}" for item in iterator]
#
# results = rdd.mapPartitions(partition)
# print(results.collect())

results = rdd.mapPartitions(
    lambda x: map(
        lambda l: f"{l}: {Random(int(time.time() * 1000) + Random().randint(0, 1000)).randint(0, 1000)}",
        x
    )
)
print(results.collect())






