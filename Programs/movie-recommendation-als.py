# -*- coding: utf-8 -*-
"""
Created on Mon Mar 12 18:33:07 2018

@author: Shreshtha
"""

from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS,Rating
import sys

def loadMovies():
    movieNames = {}
    with open("/PySpark/Resources/ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local[*]").setAppName("MovieRecommendation")
sc = SparkContext(conf = conf)

print("\nLoading Movie Names........")
nameDict = loadMovies()

data  = sc.textFile("file:///PySpark/Resources/ml-100k/u.data")
ratings = data.map(lambda x:x.split()).map(lambda x:Rating(int(x[0]),int(x[1]),float(x[2]))).cache()

print("\nTraining Recommendation Model..........")
rank = 10
iterations = 20
model = ALS.train(ratings,rank,iterations)
userId = int(sys.argv[1])

print("\n rating for user Id "+str(userId)+" : ")
userRatings = ratings.filter(lambda x : x[0]==userId)
for rating in userRatings.collect():
    print(nameDict[int(rating[1])] + " : " + str(rating[2]))

print("\nTop 10 recommendations :")
recommendations = model.recommendProducts(userId,10)
for recommendation in recommendations:
    print(nameDict[int(recommendation[1])] + " score " + str(recommendation[2]))
    
    
