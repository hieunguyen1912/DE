from pyspark import SparkContext
sc = SparkContext("local[*]", "DE - ETL - 102")

data = [1,2,3,4,5,6,7]
rdd = sc.parallelize(data)

print(rdd.take(5))
