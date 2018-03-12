# -*- coding: utf-8 -*-
"""
Created on Fri Mar  9 22:21:23 2018

@author: Shreshtha
"""

from pyspark import SparkConf,SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalizeText(text):
    return re.compile(r'\W+',re.UNICODE).split(text.lower())
    

lines = sc.textFile("file:///PySpark/Resources/Book.txt")
words = lines.flatMap(normalizeText)
wordcount = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1])
results = wordcount.collect()
for word,count in results:
    cleanword = word.encode('ascii','ignore')
    if(cleanword):
        print(cleanword,count)