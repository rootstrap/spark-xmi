import json
import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

conf = (SparkConf().set("spark.jars", "spark-xml.jar")
         .setMaster("local")
         .setAppName("My app"))
sc = SparkContext(conf = conf)
spark = SparkSession(sc)


def record(row):
    print("here")
    println(row)

df = spark.read.format('com.databricks.spark.xml').option("rowTag", "xmi:XMI").load("files/*.xmi")
print(df.rdd.count())
df.show(n=2)
df.rdd.map(record)
