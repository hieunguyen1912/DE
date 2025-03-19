from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE - ETL - 102", ).setMaster("local[*]").set("spark.execute.memory", "4g")

sc = SparkContext(conf=conf)


word = sc.parallelize(["chi nhe cai rang cua chi ra chi sat lai a chi nghiem tuc cho toi"])\
    .flatMap(lambda x: x.split(" "))\
    .map(lambda x: x.lower())

stopWords = sc.parallelize(["nhe rang chi sat nghiem"])\
    .flatMap(lambda x: x.split(" "))

finalWords = word.subtract(stopWords)
print(finalWords.collect())