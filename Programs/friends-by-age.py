# -*- coding: utf-8 -*-
"""
Created on Thu Mar  8 00:29:32 2018

@author: Shreshtha
"""

'To find count and avg of number of friends per age '

from pyspark import SparkConf,SparkContext
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age,numFriends)

lines = sc.textFile("file:///PySpark/Resources/fakefriends.csv")
rdd = lines.map(parseLine)
totalByAges = rdd.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
averageByAges = totalByAges.mapValues(lambda x : x[0]/x[1])
results = averageByAges.collect()
for result in results:
    print (result)
