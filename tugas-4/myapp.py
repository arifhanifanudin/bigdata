import os
import findspark
import math

findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql import types
spark = SparkSession.builder.appName("Python Spark Recommendation Engine").getOrCreate()

#Load the ratings file
ratings_file = os.path.join(os.getcwd(), '../../datasets/ratings.csv')
ratings = spark.read.csv(ratings_file, header=True, inferSchema=True).na.drop()
ratings.drop("timestamp")

#Load the movies file
movies_file = os.path.join(os.getcwd(), '../datasets/movies.csv')
movies = spark.read.csv(movies_file, header=True, inferSchema=True).na.drop()
movies.drop("genres")

#Cast to proper datatype
ratings = ratings.withColumn("userId", ratings["userId"].cast("int"))
ratings = ratings.withColumn("movieId", ratings["movieId"].cast("int"))
ratings = ratings.withColumn("rating", ratings["rating"].cast("float"))
movies = movies.withColumn("movieId", movies["movieId"].cast("int"))
print ("There are " + str(ratings.count()) + " recommendations in this dataset")
print ("There are " + str(movies.count()) + " movies in this dataset")

#ALS Algorithm
(training, test) = ratings.randomSplit([0.8, 0.2])
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop")
model = als.fit(training)
predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

# Generate top 10 movie recommendations for each user
#userRecs = model.recommendForAllUsers(10)

# Generate top 10 user recommendations for each movie
#movieRecs = model.recommendForAllItems(10)