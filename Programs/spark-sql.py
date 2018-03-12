# -*- coding: utf-8 -*-
"""
Created on Mon Mar 12 08:28:41 2018

@author: Shreshtha
"""

from pyspark.sql import SparkSession
from pyspark.sql import Row


spark = SparkSession.builder.config("spark.sql.warehouse.dir","file:///D:/temp").appName("SparkSQL").getOrCreate()

def parseLines(line):
    fields = line.split(",")
    return Row(ID=int(fields[0]), name=fields[1], age=int(fields[2]), numFriends=int(fields[3]))
    '''Id = int(fields[0])
    name = fields[1]
    age = int(fields[2])
    numFriends = int(fields[3])
    return Row(Id,name,age,numFriends)'''


lines = spark.sparkContext.textFile("file:///PySpark/Resources/fakefriends.csv")
people = lines.map(parseLines)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

for teen in teenagers.collect():
    print(teen)
    
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()