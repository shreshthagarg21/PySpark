# -*- coding: utf-8 -*-
"""
Created on Mon Mar 12 09:09:09 2018

@author: Shreshtha
"""

from pyspark.sql import SparkSession,Row

spark = SparkSession.builder.config("spark.sql.warehouse.dir","file:///D:/temp").appName("PopularMovieSql").getOrCreate()

def loadMovies():
    movieName = {}
    with open("/PySpark/Resources/ml-100k/u.item") as f:
        for line in f:
            fields = line.split("|")
            movieName[int(fields[0])]=fields[1]
    return movieName

nameDict = loadMovies()

lines = spark.sparkContext.textFile("file:///PySpark/Resources/ml-100k/u.data")

movies = lines.map(lambda x: Row(movieId = int(x.split()[1])))
movieDataset = spark.createDataFrame(movies)
topMoviesId = movieDataset.groupBy("movieId").count().orderBy("count",ascending=False).cache()
topMoviesId.show()
spark.stop()


