# -*- coding: utf-8 -*-
"""
Created on Sun Mar 11 11:54:15 2018

@author: Shreshtha
"""

from pyspark import SparkContext,SparkConf

conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf=conf)

def parseLines(line):
    elements = line.split()
    Id = elements[0]
    count = len(elements)-1
    return (int(Id),count)

def parseNames(name):
    fields = name.split("\"")
    Id = fields[0]
    heroname = fields[1]
    return(int(Id),heroname)

lines =sc.textFile("file:///PySpark/Resources/Marvel-Graph.txt")
parsedLines = lines.map(parseLines)
sortedConnection = parsedLines.reduceByKey(lambda x,y:x+y).sortBy(lambda x: x[1],False)
'''for res in sortedConnection:
    print (res)'''

mostPopular = sortedConnection.max(key = lambda x: x[1])
print(mostPopular)

names = sc.textFile("file:///PySpark/Resources/Marvel-Names.txt")
parsedNames = names.map(parseNames)
popularName = parsedNames.lookup(mostPopular[0])[0]
print(str(popularName))
