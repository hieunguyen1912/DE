from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE - ETL - 102", ).setMaster("local[*]").set("spark.execute.memory", "4g")

sc = SparkContext(conf=conf)

numbers = sc.parallelize([1,2,3,4,5,6,7,8,9,10], 2)

'''

'''

def sum(v1: int, v2: int) -> int:
    return v1 + v2

print(numbers.getNumPartitions())
print(numbers.reduce(sum))

