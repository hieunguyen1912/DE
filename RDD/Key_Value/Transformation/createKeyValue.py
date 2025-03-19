from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE - ETL - 102", ).setMaster("local[*]").set("spark.execute.memory", "4g")

sc = SparkContext(conf=conf)

data = sc.parallelize([(110,50.34), (120, 12.2), (130, 45.9), (100, 99.0), (140, 67.7), (110, 15.09)])

data1 = sc.parallelize([(110,"a"), (120, "b"), (130, "c"), (100, "a"), (140, "c")])

join = data.join(data1).sortByKey(ascending=False)

for res in join.collect():
    print(res)

