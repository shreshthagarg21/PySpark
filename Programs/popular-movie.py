# -*- coding: utf-8 -*-
"""
Created on Sat Mar 10 00:17:09 2018

@author: Shreshtha
"""

from pyspark import SparkContext,SparkConf

conf = SparkConf().setMaster("local").setAppName("CustomersOrders")
sc = SparkContext(conf=conf)

data = sc.textFile("file:///PySpark/Resources/ml-100k/u.data")
movieId= data.map(lambda x : x.split()[1])
moviesCount = movieId.map(lambda x:(x,1)).reduceByKey(lambda x,y : x+y).sortBy(lambda x:x[1])
results = moviesCount.collect()
for res in results:
    print (res)

'we need movie names instead of movie Id '
'broadcast variables : data is sent once and its there with each node whenever needed'
'popular-movie-better.py'


