# -*- coding: utf-8 -*-
"""
Created on Fri Mar  9 22:38:36 2018

@author: Shreshtha
"""

from pyspark import SparkContext,SparkConf

conf = SparkConf().setMaster("local").setAppName("CustomersOrders")
sc = SparkContext(conf=conf)

def parseLine(line):
    fields = line.split(",")
    custId = fields[0]
    amount = fields[2]
    return (int(custId),float(amount))

lines = sc.textFile("file:///PySpark/Resources/customer-orders.csv")
parsedlines = lines.map(parseLine)
orders = parsedlines.reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1])
results = orders.collect()
for custId,amount in results:
     print(custId,amount)
    #print(custId,str(round(amount, 2))) to round off to 2 decimal places
    
    

