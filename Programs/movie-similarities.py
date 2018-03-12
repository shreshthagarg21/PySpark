# -*- coding: utf-8 -*-
"""
Created on Sun Mar 11 18:20:58 2018

@author: Shreshtha
"""

from pyspark import SparkContext,SparkConf
import sys
from math import sqrt

def loadMovieNames():
    movieNames = {}
    with open ("/PySpark/Resources/ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])]=fields[1]
        return movieNames

def parseLines(line):
    fields = line.split()
    uid = int(fields[0])
    movieId = int(fields[1])
    rating = int(fields[2])
    return(uid,(movieId,rating))
    
def filterDuplicates(userRatings):
    ratings = userRatings[1]
    (movie1, rating1 ) = ratings[0]
    (movie2, rating2 ) = ratings[1]
    return (movie1 < movie2)
    
def makePairs(userRatings):
    ratings = userRatings[1]
    (movie1 ,rating1 ) = ratings[0]
    (movie2, rating2 ) = ratings[1]
    return ((movie1,movie2),(rating1,rating2))

def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy =0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1
        
    numerator = sum_xy
    denominator = sqrt(sum_xx)*sqrt(sum_yy)
    score = 0
    if (denominator):
        score = (numerator/(float(denominator)))
        
    return (score,numPairs)


conf = SparkConf().setMaster("local[*]").setAppName("MovieRecommendation")
sc = SparkContext(conf=conf)

print("\nLoading movie names...")
nameDict = loadMovieNames()

lines = sc.textFile("file:///PySpark/Resources/ml-100k/u.data")
parsedLines = lines.map(parseLines)
joinRatings = parsedLines.join(parsedLines)
uniqueJoinRatings = joinRatings.filter(filterDuplicates)
moviePairs = uniqueJoinRatings.map(makePairs)
moviePairRatings = moviePairs.groupByKey()
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

#moviePairSimilarities.sortByKey().saveAsTextFile("movies-similairy")

if(len(sys.argv)>1):
    scoreThreshold = 0.97
    coOccurenceThreshold = 50
    
    movieID = int(sys.argv[1])
    
    
    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter(lambda pairSim: \
        (pairSim[0][0] == movieID or pairSim[0][1] == movieID) \
        and pairSim[1][0] > scoreThreshold and pairSim[1][1] > coOccurenceThreshold)

    # Sort by quality score.
    results = filteredResults.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey(ascending = False).take(10)

    print("Top 10 similar movies for " + nameDict[movieID])
    for result in results:
        (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = pair[0]
        if (similarMovieID == movieID):
            similarMovieID = pair[1]
        print(nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))

    