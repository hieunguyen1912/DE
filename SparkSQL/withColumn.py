from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, lit, struct, udf
from pyspark.sql.types import *


'''
pyspark type
=============BASIC=============

StringType: bieu dien gia tri dang chuoi
IntegerType: bieu dien gia tri dang so nguyen 32bit
LongType: bieu dien gia tri dang so nguyen 64bit
FloatType: bieu dien gia tri dang so thuc 32bit
DoubleType: bieu dien dang so thuc 64bit
BooleanType: bieu dien gia tri dang True/False
ByteType: bieu dien gia tri dang so nguyen 8bit
ShortType: bieu dien gia tri dang so nguyen 16bit
BinaryType: bieu dien gia tri dang nhi phan
TimestampType: bieu dien gia tri dang thoi gian(ngay, gio)
DateType: bieu gia tri dang(nam/thang/ngay)

=============Advance===========


StructType: bieu dien cau truc du lieu 
StructField: bieu dien mot column trong StructType
- name: ten col
- datatype: bieu dien kieu du lieu data
- nullable: Boolean(True/False)

ArrayType(elementType): bieu dien gia tri dang Array
- elementType: bieu dien kieu du lieu cua Array(integer, string, ...)

MapType(keyType, valueType): bieu dien gia tri dang Map(key-value)
- keyType: bieu dien kieu du lieu cua key
- valueType: bieu dien kieu du lieu cua value
'''
spark = SparkSession.builder \
    .appName("DE - ETL - 102") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()



schemaType = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor", StructType([
        StructField("id", IntegerType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True)
    ]), True),
    StructField("repo", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True),
    ]), True),
    StructField("payload", StructType([
        StructField("forkee", StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("full_name", StringType(), True),
            StructField("owner", StructType([
                StructField("login", StringType(), True),
                StructField("id", IntegerType(), True),
                StructField("avatar_url", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("followers_url", StringType(), True),
                StructField("following_url", StringType(), True),
                StructField("gists_url", StringType(), True),
                StructField("starred_url", StringType(), True),
                StructField("subscriptions_url", StringType(), True),
                StructField("organizations_url", StringType(), True),
                StructField("repos_url", StringType(), True),
                StructField("events_url", StringType(), True),
                StructField("received_events_url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("site_admin", BooleanType(), True),
            ]), True),
            StructField("private", BooleanType(), True),
            StructField("html_url", StringType(), True),
            StructField("description", StringType(), True),
            StructField("fork", BooleanType(), True),
            StructField("url", StringType(), True),
            StructField("forks_url", StringType(), True),
            StructField("keys_url", StringType(), True),
            StructField("collaborators_url", StringType(), True),
            StructField("teams_url", StringType(), True),
            StructField("hooks_url", StringType(), True),
            StructField("issue_events_url", StringType(), True),
            StructField("events_url", StringType(), True),
            StructField("assignees_url", StringType(), True),
            StructField("branches_url", StringType(), True),
            StructField("tags_url", StringType(), True),
            StructField("blobs_url", StringType(), True),
            StructField("git_tags_url", StringType(), True),
            StructField("git_refs_url", StringType(), True),
            StructField("trees_url", StringType(), True),
            StructField("statuses_url", StringType(), True),
            StructField("languages_url", StringType(), True),
            StructField("stargazers_url", StringType(), True),
            StructField("contributors_url", StringType(), True),
            StructField("subscribers_url", StringType(), True),
            StructField("subscription_url", StringType(), True),
            StructField("commits_url", StringType(), True),
            StructField("git_commits_url", StringType(), True),
            StructField("comments_url", StringType(), True),
            StructField("issue_comment_url", StringType(), True),
            StructField("contents_url", StringType(), True),
            StructField("compare_url", StringType(), True),
            StructField("merges_url", StringType(), True),
            StructField("archive_url", StringType(), True),
            StructField("downloads_url", StringType(), True),
            StructField("issues_url", StringType(), True),
            StructField("pulls_url", StringType(), True),
            StructField("milestones_url", StringType(), True),
            StructField("notifications_url", StringType(), True),
            StructField("labels_url", StringType(), True),
            StructField("releases_url", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("pushed_at", TimestampType(), True),
            StructField("git_url", StringType(), True),
            StructField("ssh_url", StringType(), True),
            StructField("clone_url", StringType(), True),
            StructField("svn_url", StringType(), True),
            StructField("homepage", StringType(), True),
            StructField("size", IntegerType(), True),
            StructField("stargazers_count", IntegerType(), True),
            StructField("watchers_count", IntegerType(), True),
            StructField("language", StringType(), True),
            StructField("has_issues", BooleanType(), True),
            StructField("has_downloads", BooleanType(), True),
            StructField("has_wiki", BooleanType(), True),
            StructField("has_pages", StringType(), True),
            StructField("forks_count", IntegerType(), True),
            StructField("mirror_url", StringType(), True),
            StructField("open_issues_count", IntegerType(), True),
            StructField("forks", IntegerType(), True),
            StructField("open_issues", IntegerType(), True),
            StructField("watchers", IntegerType(), True),
            StructField("default_branch", StringType(), True),
            StructField("public", BooleanType(), True)
        ]), True)
    ]), True),
    StructField("public", BooleanType(), True),
    StructField("created_at", StringType(), True),
    StructField("org", StructType([
        StructField("id", IntegerType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True)
    ]))
])

jsonData = spark.read.schema(schemaType).json(r"C:\Users\ASUS\Documents\DE-Learning\data-20250317T120342Z-001\data\2015-03-01-17.json\2015-03-01-17.json")

'''
colName: ten cot
column: gia tri cua cot

them cot moi neu colName khong ton tai
ghi de len cot hien co ( colName ton ai )


col(), lit(), when(), struct(): de xac dinh gia tri cua cot

'''

# jsonData.withColumn("id2", lit("hdtr")).select(col("id"), col("id2")).show()
# jsonData.withColumn("actor.id2", lit("hdtr")).select(col("actor.id2")).show()


#add id2 to actor structype

# jsonDataStruct = jsonData.withColumn(
#     "actor",
#     struct(
#         col("actor.id").alias("id"),
#         col("actor.login").alias("login"),
#         col("actor.gravatar_id").alias("gravatar_id"),
#         col("actor.url").alias("url"),
#         col("actor.avatar_url").alias("avatar_url"),
#         lit("hieudeptrai").alias("id2")
#     )
# ).select(col("actor.id"), col("actor.id2")).show()

#function UDF: user define

# def add_three(data):
#     return data + 3
#
# addThreeUdf = udf(add_three)
#
# df = jsonData.withColumn("hieu", addThreeUdf(col("actor.id")))
# df.select(col("actor.id").alias("old_col"), col("hieu").alias("new_col")).show()

data = [
    ("11/12/2025",),
    ("27/02.2014",),
    ("2023.01.09",),
    ("28-12-2005",)
]

df = spark.createDataFrame(data, ["date"])
df.show()

date_schema = StructType([
    StructField("day", StringType(), True),
    StructField("month", StringType(), True),
    StructField("year", StringType(), True)
])


def parse_date(data: str) -> dict:
    if ('/' in data) and ('.' not in data):
        date = data.split('/')
        day, month, year =  date[0], date[1], date[2]
        return {"day": day, "month": month, "year": year}
    elif ('.' in data) and ('/' not in data) and ('-' not in data):
        date = data.split('.')
        year, month, day = date[0], date[1], date[2]
        return {"day": day, "month": month, "year": year}
    elif ('-' in data):
        date = data.split('-')
        day, month, year = date[0], date[1], date[2]
        return {"day": day, "month": month, "year": year}
    elif ('/' in data) and ('.' in data):
        part = data.split('/')
        day = part[0]
        date = part[1].split('.')
        month, year = date[0], date[1]
        return {"day": day, "month": month, "year": year}
    else:
        return {"day": None, "month": None, "year": None}


parse_dateUDF = udf(parse_date, date_schema)
result_df = df.withColumn("parsed_date", parse_dateUDF(df["date"]))
final_df = result_df.select(
    "date",
    "parsed_date.day",
    "parsed_date.month",
    "parsed_date.year"
)
final_df.show()







