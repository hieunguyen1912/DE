from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE - ETL - 102", ).setMaster("local[*]").set("spark.execute.memory", "4g")

sc = SparkContext(conf=conf)

fileRdd = sc.textFile("../Data/sample1.txt")
allCapsRdd = fileRdd.map(lambda x: x.upper())
wordRdd = fileRdd.flatMap(lambda line: line.split(' '))
print(wordRdd.collect())