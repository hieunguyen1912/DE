from datetime import datetime

from pyarrow.jvm import schema
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format
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

jsonData.show(truncate=False)
#jsonData.show(truncate=False)
# jsonData.select(
#     "id",
#     "type",
#     "actor.id",
#     "actor.login",
#     "actor.gravatar_id",
#     "actor.url",
#     "actor.avatar_url"
# ).show(truncate=False)