# -*- coding: utf-8 -*-
"""
Created on Fri Mar  9 21:01:55 2018

@author: Shreshtha
"""

from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///PySpark/Resources/Book.txt")
words = lines.flatMap(lambda x: x.split())
wordcount = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
results = wordcount.collect()
for word,count in results:
    cleanword = word.encode('ascii','ignore')
    if(cleanword):
        print (cleanword,count)
'''
for result in results:
    print(result)
'gives unicodEncodeError'
'''